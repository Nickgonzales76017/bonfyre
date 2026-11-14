/* Public native authoring projection: exact CMS page + tree, not HTML import.
 * Existing domain/publication owners still decide status, source rights and
 * destination release. The renderer is pure and fail-closed on unsafe input. */
#define _GNU_SOURCE
#include "site_authored_document.h"
#include "../generated/native_authoring_templates.h"
#include "../../BonfyreSurfaceScene/src/authoring_native_components.h"
#include "bf_json.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
static int route_ok(const char *route){
 if(!route||*route!='/'||strlen(route)>127||strstr(route,"..")||
    strstr(route,"//"))return 0;
 for(const unsigned char *p=(const unsigned char *)route+1;*p;p++)
  if(!((*p>='a'&&*p<='z')||(*p>='0'&&*p<='9')||
       *p=='-'||*p=='/'))return 0;
 return 1;
}
static int published_page(sqlite3 *db,const char *ns,const char *route){
 sqlite3_stmt *q=NULL;int result=-1;
 const char *sql="SELECT count(*) FROM site_page WHERE namespace=?1 AND route=?2 AND status='published'";
 if(sqlite3_prepare_v2(db,sql,-1,&q,NULL)!=SQLITE_OK)return -1;
 sqlite3_bind_text(q,1,ns,-1,SQLITE_TRANSIENT);
 sqlite3_bind_text(q,2,route,-1,SQLITE_TRANSIENT);
 if(sqlite3_step(q)==SQLITE_ROW)result=sqlite3_column_int(q,0);
 sqlite3_finalize(q);return result;
}
static int has_unreviewed_media(const char *payload,const char *source_digest,const char *evidence){
 bf_json_doc_t *doc=bf_json_parse_str(payload?payload:"{}",NULL,0);
 if(!doc)return 1;
 int media=0;
 for(int i=0;i<bf_json_count(doc);i++){
  const bf_json_node_t *node=bf_json_at(doc,i);
  if(!node||!node->key||node->type!=BF_JSON_STRING||node->key_len!=3||
     memcmp(node->key,"src",3))continue;
  int length=0;const char *raw=bf_json_get_str(node,&length);
  if(raw&&length>0){media=1;break;}
 }
 bf_json_free(doc);
 if(!media)return 0;
 /* Having a digest/evidence reference is necessary but not sufficient:
  * publication owner still verifies the actual source rights independently. */
 if(!source_digest||strlen(source_digest)!=64||!evidence||!*evidence)return 1;
 for(int i=0;i<64;i++)if(!isxdigit((unsigned char)source_digest[i]))return 1;
 return 0;
}
/* Browser and native editors share this exact eight-layout viewport grammar.
 * Unknown source tokens fail release admission; C never guesses or drops a
 * saved layout. Viewport colors, arbitrary CSS and effects remain excluded. */
static int apply_authoring_responsive(BfAuthoringTree *tree,const char *id,const char *data){
 static const char *const devices[]={"desktop","tablet","phone"};
 static const char *const layouts[]={"stack","grid","columns","carousel",
   "masonry","split","full-bleed","feature"};
 bf_json_doc_t *doc=bf_json_parse_str(data?data:"{}",NULL,0);
 if(!doc)return 0;
 const bf_json_node_t *root=bf_json_root(doc);
 int valid=root&&root->type==BF_JSON_OBJECT&&root->child_count<=3;
 for(const bf_json_node_t *device=valid?bf_json_child_first(doc,root):NULL;
    valid&&device;device=bf_json_child_next(doc,device)){
  int viewport=-1;
  for(int i=0;i<3;i++)if(device->key&&
     device->key_len==(int)strlen(devices[i])&&
     !memcmp(device->key,devices[i],(size_t)device->key_len))
    viewport=i;
  if(viewport<0||device->type!=BF_JSON_OBJECT||device->child_count>2){valid=0;break;}
  for(const bf_json_node_t *prop=bf_json_child_first(doc,device);prop;
      prop=bf_json_child_next(doc,prop)){
   if(!prop->key||
      !((prop->key_len==6&&!memcmp(prop->key,"layout",6))||
        (prop->key_len==7&&!memcmp(prop->key,"columns",7))))valid=0;
  }
  if(!valid)break;
  const bf_json_node_t *layout=bf_json_obj_get(doc,device,"layout");
  const bf_json_node_t *columns=bf_json_obj_get(doc,device,"columns");
  if(!layout){if(columns)valid=0;continue;}
  char name[24]={0};
  if(layout->type!=BF_JSON_STRING||
     bf_json_get_str_copy(layout,name,sizeof(name))<1){valid=0;break;}
  int selected=-1;
  for(int k=0;k<8;k++)if(!strcmp(name,layouts[k]))selected=k;
  if(selected<0){valid=0;break;}
  int cols=2;
  if(columns){
   if(columns->type!=BF_JSON_INT){valid=0;break;}
   const int64_t n=bf_json_get_int(columns);
   if(n<1||n>12){valid=0;break;}
   cols=(int)n;
  }
  if(!bf_authoring_tree_style(tree,id,viewport,selected,cols))valid=0;
 }
 bf_json_free(doc);return valid;
}
int bf_site_authored_document_project(sqlite3 *db,const char *site_id,
 const char *route,char *out,size_t cap,int *rendered_blocks){
 if(rendered_blocks)*rendered_blocks=0;
 if(!db||!out||cap<64||cap>4*1024*1024||!site_id||
    !bf_authoring_tree_id(site_id)||!route_ok(route))return -1;
 out[0]='\0';
 int status=published_page(db,site_id,route);
 if(status<0||status>1)return -1;
 if(!status)return 0;
 const char *sql=
  "SELECT coordinate,kind,COALESCE(heading,''),COALESCE(body,''),"
  "COALESCE(json_extract(semantic_tags,'$.authoring_v1.parent'),''),"
  "COALESCE(json_extract(semantic_tags,'$.authoring_v1.payload'),'{}'),"
  "COALESCE(source_digest,''),COALESCE(evidence_ref,''),"
  "COALESCE(json_extract(semantic_tags,'$.authoring_v1.editorial'),'[]'),"
  "COALESCE(json_extract(semantic_tags,'$.authoring_v1.responsive'),'{}'),"
  "COALESCE(json_extract(semantic_tags,'$.authoring_v1.version'),0) "
  "FROM site_fragment WHERE namespace=?1 AND page=?2 "
  "AND slot='showcase' AND audience='public' AND status='published' "
  "AND COALESCE(json_extract(projection_contract,'$.html'),0)=1 "
  "ORDER BY position,id";
 sqlite3_stmt *q=NULL;
 if(sqlite3_prepare_v2(db,sql,-1,&q,NULL)!=SQLITE_OK)return -1;
 sqlite3_bind_text(q,1,site_id,-1,SQLITE_TRANSIENT);
 sqlite3_bind_text(q,2,route,-1,SQLITE_TRANSIENT);
 BfAuthoringTree *tree=(BfAuthoringTree *)calloc(1,sizeof(*tree));
 if(!tree){sqlite3_finalize(q);return -1;}
 int ok=1,step=SQLITE_DONE;
 while((step=sqlite3_step(q))==SQLITE_ROW){
  const char *id=(const char *)sqlite3_column_text(q,0);
  const char *kind=(const char *)sqlite3_column_text(q,1);
  if(!id||!kind){ok=0;break;}
  const int authored=!strncmp(kind,"ui:",3)||!strncmp(kind,"service:",8)||
     !strncmp(kind,"profile-",8);
  if(!authored)continue;
  /* A page cannot change renderer by merely setting site_page=published.
   * Every authored fragment must carry the current typed contract. Older
   * profile blocks preserve the original public fallback until migrated. */
  if(!bf_native_authoring_template(kind)||sqlite3_column_int(q,10)!=1){
   ok=0;break;
  }
  const char *heading=(const char *)sqlite3_column_text(q,2);
  const char *body=(const char *)sqlite3_column_text(q,3);
  const char *parent=(const char *)sqlite3_column_text(q,4);
  const char *payload=(const char *)sqlite3_column_text(q,5);
  const char *digest=(const char *)sqlite3_column_text(q,6);
  const char *evidence=(const char *)sqlite3_column_text(q,7);
  const char *editorial=(const char *)sqlite3_column_text(q,8);
  const char *responsive=(const char *)sqlite3_column_text(q,9);
  if(!bf_authoring_tree_add(tree,id,kind,parent&&*parent?parent:NULL,-1)||
     !bf_authoring_tree_text(tree,id,"heading",heading?heading:"")||
     !bf_authoring_tree_text(tree,id,"body",body?body:"")||
     !bf_authoring_tree_data(tree,id,"payload",payload?payload:"{}")||
     (editorial&&strcmp(editorial,"[]")&&
      !bf_authoring_tree_data(tree,id,"editorial",editorial))||
     !apply_authoring_responsive(tree,id,responsive)||
     has_unreviewed_media(payload,digest,evidence)){
   ok=0;break;
  }
  int idx=bf_authoring_tree_index(tree,id);
  if(idx>=0)tree->nodes[idx].readonly=1;
 }
 sqlite3_finalize(q);
 if(step!=SQLITE_DONE)ok=0;
 if(ok&&tree->count){
  char *result=NULL;size_t length=0;
  FILE *buffer=open_memstream(&result,&length);
  if(!buffer)ok=0;
  else{
   const size_t budget=cap-1>BF_SITE_AUTHORED_MAX_HTML?BF_SITE_AUTHORED_MAX_HTML:cap-1;
   ok=bf_authoring_native_render(buffer,tree,budget);
   if(fclose(buffer))ok=0;
   if(ok&&length<cap){
    memcpy(out,result,length);out[length]='\0';
    if(rendered_blocks)*rendered_blocks=(int)tree->count;
   }else ok=0;
  }
  free(result);
 }
 int count=(int)tree->count;
 bf_authoring_tree_clear(tree);free(tree);
 if(!ok){out[0]='\0';return -1;}
 return count?1:0;
}
