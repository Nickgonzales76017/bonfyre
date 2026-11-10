/* Shared C authoring of Editor.js blocks. The document stays typed data;
 * only eight compiled first-party templates can render it. Markdown and inline
 * formatting remain encoded text until the separately reviewed parser stage. */
#define _GNU_SOURCE
#include "authoring_editorial.h"
#include "authoring_template.h"
#include "../../BonfyreWebAddress/generated/native_authoring_templates.h"
#include "bf_json.h"
#include "bf_jsonw.h"
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#define BF_EDITORIAL_MAX 1048576u
static int key_matches(const bf_json_node_t *node,const char *key){
 return node&&node->key&&node->key_len==(int)strlen(key)&&
  !memcmp(node->key,key,(size_t)node->key_len);
}
static int contains(const char *s,const char *match){
 const size_t n=strlen(s),m=strlen(match);
 for(size_t i=0;i+m<=n;i++){
  size_t j=0;
  while(j<m&&tolower((unsigned char)s[i+j])==
    tolower((unsigned char)match[j]))j++;
  if(j==m)return 1;
 }
 return 0;
}
static int clean_value(const bf_json_node_t *node){
 int n=0;const char *raw=bf_json_get_str(node,&n);
 if(!raw||n<0||n>32000)return 0;
 size_t bytes=0;char *s=bf_json_unescape(raw,n,&bytes);
 if(!s||strlen(s)!=bytes){free(s);return 0;}
 int ok=1;
 if(contains(s,"javascript:")||contains(s,"<script")||
    contains(s,"<iframe")||contains(s,"<style")||
    contains(s,"onerror=")||contains(s,"onclick="))ok=0;
 if(key_matches(node,"style"))
  ok=ok&&(!strcmp(s,"ordered")||!strcmp(s,"unordered"));
 if(key_matches(node,"href")||key_matches(node,"src")||key_matches(node,"url")){
  if(bytes&&s[0]=='#')ok=ok&&bytes>1&&isalpha((unsigned char)s[1]);
  else if(bytes&&s[0]=='/')ok=ok&&s[1]!='/'&&!strstr(s,"..");
  else if(bytes)ok=ok&&!strncmp(s,"https://",8)&&strchr(s+8,'.')!=NULL;
 }
 free(s);return ok;
}
static int clean_data(const bf_json_doc_t *doc,const bf_json_node_t *node,int depth){
 if(!node||depth>16)return 0;
 if(node->key){
  static const char *const deny[]={"__proto__","prototype","constructor",
   "script","html","innerHTML","outerHTML","dangerouslySetInnerHTML"};
  if(node->key_len<1||node->key_len>80)return 0;
  for(size_t i=0;i<sizeof(deny)/sizeof(*deny);i++)
   if(key_matches(node,deny[i]))return 0;
  if(node->key_len>=3&&!memcmp(node->key,"ak_",3))return 0;
  if(node->key_len>=3&&node->key[0]=='o'&&node->key[1]=='n'&&
     isupper((unsigned char)node->key[2]))return 0;
  if(key_matches(node,"style")&&node->type!=BF_JSON_STRING)return 0;
 }
 if(node->type==BF_JSON_STRING)return clean_value(node);
 if(node->type==BF_JSON_ARRAY||node->type==BF_JSON_OBJECT){
  if(node->child_count>512)return 0;
  for(const bf_json_node_t *child=bf_json_child_first(doc,node);child;
      child=bf_json_child_next(doc,child))
   if(!clean_data(doc,child,depth+1))return 0;
 }
 return 1;
}
static int emit_block(FILE *out,const bf_json_doc_t *doc,
 const bf_json_node_t *block,size_t limit){
 if(!block||block->type!=BF_JSON_OBJECT)return 0;
 const bf_json_node_t *type=bf_json_obj_get(doc,block,"type");
 const bf_json_node_t *data=bf_json_obj_get(doc,block,"data");
 char name[32]={0};
 if(!type||type->type!=BF_JSON_STRING||!data||data->type!=BF_JSON_OBJECT||
    bf_json_get_str_copy(type,name,sizeof(name))<1||
    !clean_data(doc,data,0))return 0;
 const BfNativeEditorialTemplate *template=bf_native_editorial_template(name);
 if(!template)return 0;
 const bf_json_node_t *level_field=bf_json_obj_get(doc,data,"level");
 int level=2;
 if(level_field&&level_field->type==BF_JSON_INT){
  int64_t n=bf_json_get_int(level_field);
  if(n>=2&&n<=6)level=(int)n;
 }
 const bf_json_node_t *style=bf_json_obj_get(doc,data,"style");
 int ordered=0;
 if(style&&style->type==BF_JSON_STRING){
  char value[16]={0};
  if(bf_json_get_str_copy(style,value,sizeof(value))<1)return 0;
  ordered=!strcmp(value,"ordered");
 }
 bf_jsonw w;bf_jsonw_init(&w,0);bf_jsonw_obj(&w);
 bf_jsonw_key(&w,"data");bf_jsonw_node(&w,doc,data,0);
 bf_jsonw_key(&w,"ak_ordered");bf_jsonw_bool(&w,ordered);
 for(int h=2;h<=6;h++){
  char key[12];snprintf(key,sizeof(key),"ak_h%d",h);
  bf_jsonw_key(&w,key);bf_jsonw_bool(&w,h==level);
 }
 bf_jsonw_obj_end(&w);
 int ok=!w.oom&&w.buf&&w.len<=140000&&
  bf_authoring_template_render(out,template->markup,w.buf,limit);
 bf_jsonw_free(&w);return ok;
}
int bf_authoring_editorial_render(FILE *out,const char *editorial,size_t maximum){
 if(!out||!editorial||maximum<1||maximum>BF_EDITORIAL_MAX||
    strlen(editorial)>100000)return 0;
 bf_json_doc_t *doc=bf_json_parse_str(editorial,NULL,0);
 if(!doc)return 0;
 const bf_json_node_t *root=bf_json_root(doc);
 if(!root||root->type!=BF_JSON_ARRAY||root->child_count>256||
    bf_json_count(doc)>2500){bf_json_free(doc);return 0;}
 char *result=NULL;size_t total=0;
 FILE *buffer=open_memstream(&result,&total);
 if(!buffer){bf_json_free(doc);return 0;}
 int ok=1;
 for(int i=0;i<root->child_count&&ok;i++){
  const bf_json_node_t *block=bf_json_arr_get(doc,root,i);
  if(total>=maximum){ok=0;break;}
  /* open_memstream publishes its current length on flush. Every block gets
   * only the remaining budget, not a fresh megabyte allowance. */
  ok=emit_block(buffer,doc,block,maximum-total);
  if(ok&&fflush(buffer)!=0)ok=0;
  if(total>maximum)ok=0;
 }
 if(fclose(buffer))ok=0;
 bf_json_free(doc);
 if(ok&&total<=maximum)ok=fwrite(result,1,total,out)==total;
 else ok=0;
 free(result);return ok;
}
