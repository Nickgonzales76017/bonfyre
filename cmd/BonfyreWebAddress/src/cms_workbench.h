#ifndef BONFYRE_CMS_WORKBENCH_H
#define BONFYRE_CMS_WORKBENCH_H
#include "cms_editor.h"
/* Presentation over native CMS schema, records and conditional writes. */
#define BF_CMS_WORKBENCH_STYLE \
"<link rel=\"stylesheet\" href=\"/assets/cms-workbench.css\">"
#define BF_CMS_WORKBENCH_SCRIPT BF_CMS_EDITOR_SCRIPT
static int cms_field_metadata(sqlite3 *db,const char *type,const char *field,char *kind,size_t cap,int *required){
 sqlite3_stmt *st=NULL;int found=0;if(kind&&cap)kind[0]=0;if(required)*required=0;
 if(!db||!type||!field||!kind||cap<2||!studio_safe_ident(field))return 0;
 if(sqlite3_prepare_v2(db,"SELECT json_extract(schema_json,'$.attributes.'||?2||'.type'),COALESCE(json_extract(schema_json,'$.attributes.'||?2||'.required'),0) FROM _content_types WHERE name=?1",-1,&st,NULL)!=SQLITE_OK)return 0;
 sqlite3_bind_text(st,1,type,-1,SQLITE_TRANSIENT);sqlite3_bind_text(st,2,field,-1,SQLITE_TRANSIENT);
 if(sqlite3_step(st)==SQLITE_ROW){const char *v=(const char*)sqlite3_column_text(st,0);if(v&&strlen(v)<cap){snprintf(kind,cap,"%s",v);if(required)*required=sqlite3_column_int(st,1);found=1;}}
 sqlite3_finalize(st);return found;
}
static void cms_human_field(const char *machine,char *out,size_t size){size_t i=0;if(!out||!size)return;for(;machine&&*machine&&i+1<size;machine++){char c=*machine=='_'?' ':*machine;if(!i)c=(char)toupper((unsigned char)c);out[i++]=c;}out[i]=0;}
#endif
