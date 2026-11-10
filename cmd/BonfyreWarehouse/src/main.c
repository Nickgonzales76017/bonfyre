#define _POSIX_C_SOURCE 200809L
/*
 * BonfyreWarehouse: native, read-only inspection of derived warehouse
 * snapshot heads in the canonical Representation Manifold.
 *
 * Does not install/bind heads, execute arbitrary SQL, or open physical data.
 * The existing manifold head owner is the only writer.
 */
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <limits.h>

static void json_string(const char *s) {
  putchar('"');
  if (!s) s = "";
  for (const unsigned char *p = (const unsigned char *)s; *p; p++) {
    if (*p == '"' || *p == '\\') { putchar('\\'); putchar((char)*p); }
    else if (*p < 32) printf("\\u%04x", *p);
    else putchar((char)*p);
  }
  putchar('"');
}
static int key(const char *s) {
  size_t n = s ? strlen(s) : 0;
  if (!n || n > 240) return 0;
  for (size_t i = 0; i < n; i++) {
    unsigned char c = (unsigned char)s[i];
    if (!((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
        (c >= '0' && c <= '9') || c == '_' || c == '.' || c == ':' ||
        c == '/' || c == '-')) return 0;
  }
  return 1;
}
static int table_exists(sqlite3 *db, const char *table) {
  sqlite3_stmt *stmt = NULL;
  int exists = 0;
  if (sqlite3_prepare_v2(db, "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", -1, &stmt, 0) != SQLITE_OK)
    return -1;
  sqlite3_bind_text(stmt, 1, table, -1, SQLITE_TRANSIENT);
  int rc = sqlite3_step(stmt);
  if (rc == SQLITE_ROW) exists = 1;
  else if (rc != SQLITE_DONE) exists = -1;
  sqlite3_finalize(stmt);
  return exists;
}
static int count_rows(sqlite3 *db, const char *sql, sqlite3_int64 *value) {
  sqlite3_stmt *stmt = NULL;
  if (sqlite3_prepare_v2(db, sql, -1, &stmt, 0) != SQLITE_OK) return 0;
  int rc = sqlite3_step(stmt);
  if (rc == SQLITE_ROW) *value = sqlite3_column_int64(stmt, 0);
  sqlite3_finalize(stmt);
  return rc == SQLITE_ROW;
}
static int status(sqlite3 *db) {
  sqlite3_int64 instances = 0, physical = 0, binding = 0, heads = 0;
  int installed = table_exists(db, "warehouse_snapshot_head");
  if (installed < 0 || table_exists(db, "representation_instance") != 1 ||
      table_exists(db, "physical_realization") != 1 ||
      table_exists(db, "source_binding") != 1) return 0;
  if (!count_rows(db, "SELECT COUNT(*) FROM representation_instance", &instances) ||
      !count_rows(db, "SELECT COUNT(*) FROM physical_realization WHERE available=1", &physical) ||
      !count_rows(db, "SELECT COUNT(*) FROM source_binding", &binding)) return 0;
  if (installed && !count_rows(db, "SELECT COUNT(*) FROM warehouse_snapshot_head", &heads)) return 0;
  printf("{\"state\":\"READ_ONLY\",\"warehouse_heads_installed\":%s,"
         "\"heads\":%lld,\"representation_instances\":%lld,"
         "\"available_realizations\":%lld,\"source_bindings\":%lld,"
         "\"physical_bytes_verified\":false,\"execution_authority\":false}\n",
         installed ? "true" : "false", (long long)heads, (long long)instances,
         (long long)physical, (long long)binding);
  return 1;
}
/* Classify unreachable metadata, never delete it. Historical receipts and
 * representations may legitimately outlive their materialized bodies. */
static int integrity(sqlite3 *db) {
  sqlite3_int64 dangling_body = 0, dangling_edge = 0, dangling_receipt = 0, stale_head = 0;
  if (table_exists(db, "representation_instance") != 1 ||
      table_exists(db, "physical_realization") != 1 ||
      table_exists(db, "representation_edge") != 1 ||
      table_exists(db, "eviction_receipt") != 1 ||
      table_exists(db, "source_binding") != 1) return 0;
  if (!count_rows(db, "SELECT COUNT(*) FROM physical_realization p "
      "WHERE NOT EXISTS(SELECT 1 FROM representation_instance ri WHERE ri.id=p.representation_id)", &dangling_body) ||
      !count_rows(db, "SELECT COUNT(*) FROM representation_edge e WHERE "
      "NOT EXISTS(SELECT 1 FROM representation_instance ri WHERE ri.id=e.parent_representation_id) "
      "OR NOT EXISTS(SELECT 1 FROM representation_instance ri WHERE ri.id=e.child_representation_id)",
      &dangling_edge) ||
      !count_rows(db, "SELECT COUNT(*) FROM eviction_receipt er "
      "WHERE NOT EXISTS(SELECT 1 FROM representation_instance ri WHERE ri.id=er.representation_id)",
      &dangling_receipt)) return 0;
  if (table_exists(db, "warehouse_snapshot_head") == 1 &&
      !count_rows(db, "SELECT COUNT(*) FROM warehouse_snapshot_head h "
        "WHERE (SELECT COUNT(*) FROM source_binding sb "
        "WHERE sb.semantic_kind=h.semantic_kind)!=1 "
        "OR NOT EXISTS(SELECT 1 FROM representation_target rt "
        "WHERE rt.semantic_kind=h.semantic_kind "
        "AND rt.representation_kind=h.representation_kind AND rt.status='physical') "
        "OR NOT EXISTS(SELECT 1 FROM representation_instance ri WHERE ri.id=h.representation_id "
        "AND ri.digest=h.digest AND ri.frontier=h.frontier AND ri.state='physical' "
        "AND EXISTS(SELECT 1 FROM physical_realization pr WHERE pr.representation_id=ri.id "
        "AND pr.available=1 AND pr.digest=ri.digest))", &stale_head)) return 0;
  printf("{\"state\":\"METADATA_INTEGRITY_OBSERVED\",\"dangling_realizations\":%lld,"
    "\"dangling_edges\":%lld,\"dangling_eviction_receipts\":%lld,"
    "\"stale_heads\":%lld,\"deleted\":0,\"safe_to_delete_inferred\":false}\n",
    (long long)dangling_body, (long long)dangling_edge,
    (long long)dangling_receipt, (long long)stale_head);
  return 1;
}

#include "cohort_audit.h"
#include "warehouse_verify.h"
#include "warehouse_query_plan.h"
#include "warehouse_retention.h"
#include "warehouse_portfolio.h"
static int head(sqlite3 *db, const char *semantic, const char *kind, int explain) {
  if (!key(semantic) || !key(kind)) return 0;
  if (table_exists(db, "warehouse_snapshot_head") != 1) {
    puts("{\"state\":\"NOT_INSTALLED\",\"head\":null}");
    return 1;
  }
  const char *select_sql = "SELECT h.generation,h.representation_id,h.digest,h.frontier,h.created_at,"
    "EXISTS(SELECT 1 FROM representation_instance ri WHERE ri.id=h.representation_id "
    "AND ri.schema_ref=h.semantic_kind AND ri.kind=h.representation_kind "
    "AND ri.digest=h.digest AND ri.frontier=h.frontier AND ri.created_at=h.created_at "
    "AND ri.state='physical' AND EXISTS(SELECT 1 FROM physical_realization pr "
    "WHERE pr.representation_id=ri.id AND pr.available=1 AND pr.digest=ri.digest)) "
    "AND (SELECT COUNT(*) FROM source_binding sb WHERE sb.semantic_kind=h.semantic_kind)=1 "
    "AND EXISTS(SELECT 1 FROM representation_target rt WHERE rt.semantic_kind=h.semantic_kind "
    "AND rt.representation_kind=h.representation_kind AND rt.status='physical') "
    "FROM warehouse_snapshot_head h WHERE h.semantic_kind=? AND h.representation_kind=?";
  const char *plan_sql = "EXPLAIN QUERY PLAN SELECT generation FROM warehouse_snapshot_head "
    "WHERE semantic_kind=? AND representation_kind=?";
  sqlite3_stmt *stmt = NULL;
  if (sqlite3_prepare_v2(db, explain ? plan_sql : select_sql, -1, &stmt, 0) != SQLITE_OK)
    return 0;
  sqlite3_bind_text(stmt, 1, semantic, -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt, 2, kind, -1, SQLITE_TRANSIENT);
  int rc = sqlite3_step(stmt);
  if (explain) {
    if (rc == SQLITE_ROW) {
      const char *plan = (const char *)sqlite3_column_text(stmt, 3);
      printf("{\"state\":\"QUERY_PLAN\",\"plan\":");
      json_string(plan);
      puts(",\"physical_bytes_verified\":false}");
    }
  } else if (rc == SQLITE_ROW) {
    if (sqlite3_column_int(stmt, 5) != 1) {
      puts("{\"state\":\"STALE_HEAD_REFUSED\",\"head\":null}");
    } else {
      printf("{\"state\":\"METADATA_HEAD_CURRENT\",\"generation\":%lld,\"representation_id\":",
        (long long)sqlite3_column_int64(stmt, 0));
      json_string((const char *)sqlite3_column_text(stmt, 1));
      printf(",\"digest\":"); json_string((const char *)sqlite3_column_text(stmt, 2));
      printf(",\"frontier\":"); json_string((const char *)sqlite3_column_text(stmt, 3));
      puts(",\"physical_bytes_verified\":false,\"execution_authority\":false}");
    }
  } else if (rc == SQLITE_DONE) {
    puts("{\"state\":\"NO_HEAD\",\"head\":null}");
  }
  int good = (rc == SQLITE_ROW || rc == SQLITE_DONE);
  sqlite3_finalize(stmt);
  return good;
}
static int usage(void) {
  fputs("usage: bonfyre-warehouse status|integrity|cohorts|verify --estate ABSOLUTE_DIR\n"
        "       bonfyre-warehouse portfolio --estate ABSOLUTE_DIR --offset N --limit 1..100\n"
        "       bonfyre-warehouse retention --estate ABSOLUTE_DIR --cutoff YYYY-MM-DDTHH:MM:SSZ\n"
        "       bonfyre-warehouse head|plan --estate ABSOLUTE_DIR --semantic-kind NAME --kind FORMAT\n"
        "       bonfyre-warehouse query-plan --estate ABSOLUTE_DIR --semantic-kind NAME --kind parquet\n"
        "         --operation scan|aggregate|join --generation N --input-bound-bytes N\n"
        "         --memory-limit-bytes N --workers N [--right-semantic-kind NAME\n"
        "         --right-generation N --right-input-bound-bytes N]\n", stderr);
  return 2;
}
int main(int argc, char **argv) {
  const char *action = argc > 1 ? argv[1] : NULL;
  const char *estate = NULL, *semantic = NULL, *kind = NULL;
  const char *operation=NULL,*generation=NULL,*input_bound=NULL,*memory=NULL,*workers=NULL;
  const char *right_semantic=NULL,*right_generation=NULL,*right_bound=NULL,*cutoff=NULL,*offset=NULL,*limit=NULL;
  int planning=action && !strcmp(action,"query-plan");
  if (!action || (strcmp(action, "status") && strcmp(action, "integrity") && strcmp(action, "cohorts") && strcmp(action, "verify") &&
                  strcmp(action, "head") && strcmp(action, "plan") && strcmp(action, "query-plan") &&
                  strcmp(action, "retention") && strcmp(action, "portfolio")))
    return usage();
  for (int i = 2; i < argc; i += 2) {
    if (i + 1 >= argc) return usage();
    if (!strcmp(argv[i], "--estate") && !estate) estate = argv[i+1];
    else if (!strcmp(argv[i], "--semantic-kind") && !semantic) semantic = argv[i+1];
    else if (!strcmp(argv[i], "--kind") && !kind) kind = argv[i+1];
    else if (!strcmp(action,"portfolio") && !strcmp(argv[i],"--offset") && !offset) offset=argv[i+1];
    else if (!strcmp(action,"portfolio") && !strcmp(argv[i],"--limit") && !limit) limit=argv[i+1];
    else if (!strcmp(action,"retention") && !strcmp(argv[i],"--cutoff") && !cutoff) cutoff=argv[i+1];
    else if (planning && !strcmp(argv[i],"--operation") && !operation) operation=argv[i+1];
    else if (planning && !strcmp(argv[i],"--generation") && !generation) generation=argv[i+1];
    else if (planning && !strcmp(argv[i],"--input-bound-bytes") && !input_bound) input_bound=argv[i+1];
    else if (planning && !strcmp(argv[i],"--memory-limit-bytes") && !memory) memory=argv[i+1];
    else if (planning && !strcmp(argv[i],"--workers") && !workers) workers=argv[i+1];
    else if (planning && !strcmp(argv[i],"--right-semantic-kind") && !right_semantic) right_semantic=argv[i+1];
    else if (planning && !strcmp(argv[i],"--right-generation") && !right_generation) right_generation=argv[i+1];
    else if (planning && !strcmp(argv[i],"--right-input-bound-bytes") && !right_bound) right_bound=argv[i+1];
    else return usage();
  }
  if (!estate || estate[0] != '/' || strlen(estate) >= PATH_MAX - 40) return usage();
  if (!strcmp(action, "status") || !strcmp(action, "integrity") || !strcmp(action, "cohorts") || !strcmp(action, "verify") || !strcmp(action,"retention") || !strcmp(action,"portfolio")) {
    if (semantic || kind || (!strcmp(action,"retention") && !cutoff) || (!strcmp(action,"portfolio") && (!offset||!limit))) return usage();
  }
  else if (!semantic || !kind) return usage();
  if (planning && (!operation || !generation || !input_bound || !memory || !workers ||
                   strcmp(kind,"parquet"))) return usage();
  char path[PATH_MAX];
  if (snprintf(path, sizeof(path), "%s/representation-manifold.db", estate) >= (int)sizeof(path))
    return usage();
  struct stat st;
  if (lstat(path, &st) != 0 || !S_ISREG(st.st_mode)) {
    fputs("manifold must be an existing regular file (not a symlink)\n", stderr);
    return 1;
  }
  sqlite3 *db = NULL;
  if (sqlite3_open_v2(path, &db, SQLITE_OPEN_READONLY | SQLITE_OPEN_NOMUTEX, NULL) != SQLITE_OK) {
    if (db) sqlite3_close(db);
    fputs("cannot open manifold read-only\n", stderr);
    return 1;
  }
  if (sqlite3_exec(db, "BEGIN", NULL, NULL, NULL) != SQLITE_OK) {
    sqlite3_close(db);
    fputs("cannot begin read snapshot\n", stderr);
    return 1;
  }
  int ok = !strcmp(action, "status") ? status(db) :
    !strcmp(action, "integrity") ? integrity(db) :
    !strcmp(action, "cohorts") ? warehouse_cohorts(db) :
    !strcmp(action, "verify") ? warehouse_verify(db) :
    !strcmp(action, "retention") ? warehouse_retention(db,cutoff) :
    !strcmp(action, "portfolio") ? warehouse_portfolio(db,offset,limit) :
    planning ? warehouse_query_plan(db,operation,
      (warehouse_query_input){semantic,kind,generation,input_bound},
      (warehouse_query_input){right_semantic,"parquet",right_generation,right_bound},
      memory,workers) : head(db, semantic, kind, !strcmp(action, "plan"));
  if (sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL) != SQLITE_OK) ok = 0;
  sqlite3_close(db);
  if (!ok) fputs("invalid or unavailable manifold metadata\n", stderr);
  return ok ? 0 : 1;
}
