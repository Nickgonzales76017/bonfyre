#include "bridge_parse.h"
static int bproject(sqlite3*j,const char*path,const char*out){size_t n=0;char*b=bread(path,&n),*delta=NULL,*write=NULL,*encoded=NULL;sqlite3_stmt*q=NULL;BCondition v={0};int ok=0;if(!b)return -1;if(!bc_parse(j,b,n,&v))goto done;
 {size_t cap=strlen(v.condition)+strlen(v.subject)+strlen(v.scope)+96;delta=malloc(cap);write=malloc(cap);if(!delta||!write||snprintf(delta,cap,"pack-condition:%s:%s:%s:%lld",v.condition,v.subject,v.scope,(long long)v.generation)>=(int)cap||snprintf(write,cap,"PackCondition:%s:%s:%s",v.condition,v.subject,v.scope)>=(int)cap)goto done;}
 if(sqlite3_prepare_v2(j,"SELECT json_object('schema','bonfyre-fact-delta.v1','delta_id',?1,'writes',json_array(?2),'source_schema','bonfyre-pack-condition-delta.v1','condition_ref',?3,'subject',?4,'scope',?5,'state',?6,'generation',?7,'basis',?8,'truth','condition_change_projection_only')",-1,&q,NULL)!=SQLITE_OK)goto done;
 sqlite3_bind_text(q,1,delta,-1,SQLITE_TRANSIENT);sqlite3_bind_text(q,2,write,-1,SQLITE_TRANSIENT);sqlite3_bind_text(q,3,v.condition,-1,SQLITE_TRANSIENT);sqlite3_bind_text(q,4,v.subject,-1,SQLITE_TRANSIENT);sqlite3_bind_text(q,5,v.scope,-1,SQLITE_TRANSIENT);sqlite3_bind_text(q,6,v.state,-1,SQLITE_TRANSIENT);sqlite3_bind_int64(q,7,v.generation);sqlite3_bind_text(q,8,v.basis,-1,SQLITE_TRANSIENT);
 if(sqlite3_step(q)==SQLITE_ROW&&sqlite3_column_text(q,0))encoded=strdup((const char*)sqlite3_column_text(q,0));
 if(encoded&&bpublish(out,delta,encoded))ok=1;
done:sqlite3_finalize(q);free(b);free(delta);free(write);free(encoded);bc_free(&v);return ok;
}
