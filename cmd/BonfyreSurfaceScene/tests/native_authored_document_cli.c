/* Test carrier only: read an exact SQLite CMS snapshot and project the
 * native C authored document. This binary never writes, publishes or resolves
 * an account. It allows browser tests to exercise actual C-generated HTML. */
#include "../../BonfyreWebAddress/src/site_authored_document.h"
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
int main(int argc,char **argv){
 if(argc!=4)return 64;
 sqlite3 *db=NULL;
 if(sqlite3_open_v2(argv[1],&db,SQLITE_OPEN_READONLY,NULL)!=SQLITE_OK){
  if(db)sqlite3_close(db);
  return 65;
 }
 const size_t cap=1048577;
 char *buffer=(char *)malloc(cap);
 if(!buffer){sqlite3_close(db);return 70;}
 int blocks=0;
 int rc=bf_site_authored_document_project(db,argv[2],argv[3],buffer,cap,&blocks);
 int ok=(rc==1&&blocks>0&&fputs(buffer,stdout)>=0);
 free(buffer);sqlite3_close(db);
 return ok?0:66;
}
