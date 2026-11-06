#define _POSIX_C_SOURCE 200809L
/* Effect-capable warehouse migration owner, separated from read-only diagnostics.
 * Native admission must be obtained independently before any estate mutation. */
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <limits.h>
static int table_exists(sqlite3 *db, const char *name) {
  sqlite3_stmt *stmt=NULL;
  if(sqlite3_prepare_v2(db,"SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", -1,&stmt,NULL)!=SQLITE_OK)
    return -1;
  sqlite3_bind_text(stmt,1,name,-1,SQLITE_TRANSIENT);
  int rc=sqlite3_step(stmt);
  int ok=rc==SQLITE_ROW?1:rc==SQLITE_DONE?0:-1;
  sqlite3_finalize(stmt);
  return ok;
}
static int count_rows(sqlite3 *db,const char *sql,sqlite3_int64 *out) {
  sqlite3_stmt *stmt=NULL;
  if(sqlite3_prepare_v2(db,sql,-1,&stmt,NULL)!=SQLITE_OK) return 0;
  int rc=sqlite3_step(stmt);
  if(rc==SQLITE_ROW)*out=sqlite3_column_int64(stmt,0);
  sqlite3_finalize(stmt);
  return rc==SQLITE_ROW;
}
#include "../../BonfyreWarehouse/src/warehouse_seed.h"
static int usage(void) {
  fputs("usage: bonfyre-warehouse-seed seed --apply --estate ABSOLUTE_DIR\n",stderr);
  return 2;
}
int main(int argc,char **argv) {
  const char *estate=NULL;
  if(argc!=5 || strcmp(argv[1],"seed")) return usage();
  if(!strcmp(argv[2],"--apply") && !strcmp(argv[3],"--estate")) estate=argv[4];
  else if(!strcmp(argv[2],"--estate") && !strcmp(argv[4],"--apply")) estate=argv[3];
  else return usage();
  if(!estate||estate[0]!='/'||strlen(estate)>=PATH_MAX-40) return usage();
  char path[PATH_MAX];
  if(snprintf(path,sizeof(path),"%s/representation-manifold.db",estate)>=(int)sizeof(path))
    return usage();
  struct stat st;
  if(lstat(path,&st)!=0 || !S_ISREG(st.st_mode)) {
    fputs("manifold must exist as a regular non-symlink file\n",stderr);
    return 1;
  }
  sqlite3 *db=NULL;
  if(sqlite3_open_v2(path,&db,SQLITE_OPEN_READWRITE|SQLITE_OPEN_NOMUTEX,NULL)!=SQLITE_OK){
    if(db) sqlite3_close(db);
    fputs("cannot open manifold for admitted migration\n",stderr);
    return 1;
  }
  int ok=warehouse_seed(db);
  sqlite3_close(db);
  return ok?0:1;
}
