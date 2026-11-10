#include "bridge_io.h"
typedef struct { char *condition,*subject,*scope,*state,*basis; sqlite3_int64 generation; } BCondition;
static void bc_free(BCondition *v){if(!v)return;free(v->condition);free(v->subject);free(v->scope);free(v->state);free(v->basis);memset(v,0,sizeof(*v));}
static int bc_parse(sqlite3*j,const char*body,size_t n,BCondition*out){sqlite3_stmt*q=NULL;int ok=0;memset(out,0,sizeof(*out));
 if(sqlite3_prepare_v2(j,"SELECT json_extract(?1,'$.schema'),json_extract(?1,'$.condition_ref'),json_extract(?1,'$.subject'),json_extract(?1,'$.scope'),json_extract(?1,'$.state'),json_extract(?1,'$.generation'),json_extract(?1,'$.basis'),json_extract(?1,'$.truth')",-1,&q,NULL)!=SQLITE_OK)return 0;
 sqlite3_bind_text(q,1,body,(int)n,SQLITE_TRANSIENT);
 if(sqlite3_step(q)==SQLITE_ROW){const char*schema=(const char*)sqlite3_column_text(q,0),*c=(const char*)sqlite3_column_text(q,1),*s=(const char*)sqlite3_column_text(q,2),*scope=(const char*)sqlite3_column_text(q,3),*state=(const char*)sqlite3_column_text(q,4),*basis=(const char*)sqlite3_column_text(q,6),*truth=(const char*)sqlite3_column_text(q,7);sqlite3_int64 gen=sqlite3_column_int64(q,5);
  if(schema&&!strcmp(schema,"bonfyre-pack-condition-delta.v1")&&c&&*c&&s&&*s&&scope&&*scope&&state&&*state&&gen>=1&&basis&&*basis&&truth&&!strcmp(truth,"accepted_condition_change_notification_not_authority")){out->condition=strdup(c);out->subject=strdup(s);out->scope=strdup(scope);out->state=strdup(state);out->basis=strdup(basis);out->generation=gen;ok=out->condition&&out->subject&&out->scope&&out->state&&out->basis;}}
 sqlite3_finalize(q);
 if(!ok)bc_free(out);
 return ok;
}
