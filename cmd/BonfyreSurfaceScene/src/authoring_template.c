/* Shared native template renderer, using upstream mini-mustach under 0BSD.
 * The authoritative publisher must still check audience, sources and state.
 * Do NOT pass editor-exported templates here as trusted code. */
#define _GNU_SOURCE
#include "authoring_template.h"
#include "../vendor/mustach/mini-mustach.h"
#include "bf_json.h"
#include "bf_jsonw.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <ctype.h>
#define BF_TMPL_DEPTH 24
typedef struct {
 const bf_json_node_t *prior;
 const bf_json_node_t *array;
 int index;
} BfTplFrame;
typedef struct {
 FILE *out;
 size_t used,max;
 const bf_json_doc_t *doc;
 const bf_json_node_t *root,*scope;
 BfTplFrame frames[BF_TMPL_DEPTH];
 int depth;
} BfTpl;
static int bf_tmpl_write(BfTpl *r,const char *ptr,size_t n){
 if(n>r->max||r->used>r->max-n)return MUSTACH_ERROR_TOO_BIG;
 if(n&&fwrite(ptr,1,n,r->out)!=n)return MUSTACH_ERROR_SYSTEM;
 r->used+=n;return MUSTACH_OK;
}
static int bf_tmpl_emit(void *closure,const char *text,size_t n,int escape){
 BfTpl *r=(BfTpl *)closure;
 if(!escape)return bf_tmpl_write(r,text,n);
 for(size_t i=0;i<n;i++){
  const char *entity=NULL;
  switch(text[i]){
   case '&':entity="&amp;";break;
   case '<':entity="&lt;";break;
   case '>':entity="&gt;";break;
   case '"':entity="&quot;";break;
   case 39:entity="&#39;";break;
  }
  int rc=entity?bf_tmpl_write(r,entity,strlen(entity)):bf_tmpl_write(r,text+i,1);
  if(rc!=MUSTACH_OK)return rc;
 }
 return MUSTACH_OK;
}
static const bf_json_node_t *bf_tmpl_in_scope(BfTpl *r,const bf_json_node_t *scope,
                                               const char *name,size_t len){
 char key[128];
 if(!scope||!len||len>=sizeof(key))return NULL;
 if(len==1&&name[0]=='.')return scope;
 memcpy(key,name,len);key[len]='\0';
 const bf_json_node_t *node=scope;
 char *part=key;
 for(char *p=key;;p++){
  if(*p!='.'&&*p!='\0')continue;
  const int more=(*p=='.');
  *p='\0';
  if(!*part||!node||node->type!=BF_JSON_OBJECT)return NULL;
  node=bf_json_obj_get(r->doc,node,part);
  if(!more)return node;
  part=p+1;
 }
}
static const bf_json_node_t *bf_tmpl_find(BfTpl *r,const char *name,size_t len){
 const bf_json_node_t *node=bf_tmpl_in_scope(r,r->scope,name,len);
 if(node)return node;
 for(int i=r->depth-1;i>=0;i--){
  node=bf_tmpl_in_scope(r,r->frames[i].prior,name,len);
  if(node)return node;
 }
 return bf_tmpl_in_scope(r,r->root,name,len);
}
static int bf_tmpl_get(void *closure,const char *name,size_t len,mustach_sbuf_t *sbuf){
 BfTpl *r=(BfTpl *)closure;
 memset(sbuf,0,sizeof(*sbuf));
 const bf_json_node_t *node=bf_tmpl_find(r,name,len);
 if(!node||node->type==BF_JSON_NULL||node->type==BF_JSON_OBJECT||node->type==BF_JSON_ARRAY)return MUSTACH_OK;
 char number[80];const char *value=NULL;size_t bytes=0;
 if(node->type==BF_JSON_STRING){
  int text_len=0;const char *raw=bf_json_get_str(node,&text_len);
  if(!raw||text_len<0||text_len>50000)return MUSTACH_ERROR_BAD_DATA;
  char *decoded=bf_json_unescape(raw,text_len,&bytes);
  if(!decoded)return MUSTACH_ERROR_OUT_OF_MEMORY;
  sbuf->value=decoded;sbuf->length=bytes;sbuf->freecb=free;return MUSTACH_OK;
 }
 if(node->type==BF_JSON_BOOL)value=bf_json_get_bool(node)?"true":"false";
 else if(node->type==BF_JSON_INT){
  snprintf(number,sizeof(number),"%lld",(long long)bf_json_get_int(node));value=number;
 }else if(node->type==BF_JSON_DOUBLE){
  snprintf(number,sizeof(number),"%.15g",bf_json_get_double(node));value=number;
 }else return MUSTACH_ERROR_BAD_DATA;
 bytes=strlen(value);
 char *copy=(char *)malloc(bytes+1);
 if(!copy)return MUSTACH_ERROR_OUT_OF_MEMORY;
 memcpy(copy,value,bytes+1);
 sbuf->value=copy;sbuf->length=bytes;sbuf->freecb=free;return MUSTACH_OK;
}
static int bf_tmpl_enter(void *closure,const char *name,size_t len){
 BfTpl *r=(BfTpl *)closure;
 if(r->depth>=BF_TMPL_DEPTH)return 0;
 const bf_json_node_t *node=bf_tmpl_find(r,name,len);
 if(!node||node->type==BF_JSON_NULL)return 0;
 if(node->type==BF_JSON_BOOL&&!bf_json_get_bool(node))return 0;
 if(node->type==BF_JSON_STRING&&node->val.str.len==0)return 0;
 if(node->type==BF_JSON_ARRAY&&node->child_count==0)return 0;
 BfTplFrame *frame=&r->frames[r->depth++];
 frame->prior=r->scope;frame->array=NULL;frame->index=0;
 if(node->type==BF_JSON_ARRAY){
  frame->array=node;r->scope=bf_json_arr_get(r->doc,node,0);
 }else if(node->type==BF_JSON_OBJECT||node->type==BF_JSON_STRING)r->scope=node;
 return 1;
}
static int bf_tmpl_next(void *closure){
 BfTpl *r=(BfTpl *)closure;
 if(!r->depth)return 0;
 BfTplFrame *frame=&r->frames[r->depth-1];
 if(!frame->array||++frame->index>=frame->array->child_count)return 0;
 r->scope=bf_json_arr_get(r->doc,frame->array,frame->index);
 return r->scope!=NULL;
}
static int bf_tmpl_leave(void *closure){
 BfTpl *r=(BfTpl *)closure;
 if(!r->depth)return MUSTACH_ERROR_CLOSING;
 r->scope=r->frames[--r->depth].prior;return MUSTACH_OK;
}
int bf_authoring_template_render(FILE *out,const char *trusted_template,
                                 const char *json_data,size_t max_output){
 if(!out||!trusted_template||!json_data||!max_output||max_output>1048576||
    strlen(trusted_template)>65536||strlen(json_data)>220000||!max_output)return 0;
 /* No raw or user-supplied template interpolation. Dynamic text always escapes. */
 if(strstr(trusted_template,"{{{")||strstr(trusted_template,"{{&")||
    strstr(trusted_template,"{{=")||strstr(trusted_template,"<script")||
    strstr(trusted_template,"<iframe")||strstr(trusted_template," onerror="))return 0;
 bf_json_doc_t *doc=bf_json_parse_str(json_data,NULL,0);
 if(!doc)return 0;
 const bf_json_node_t *root=bf_json_root(doc);
 if(!root||root->type!=BF_JSON_OBJECT||bf_json_count(doc)>4000){bf_json_free(doc);return 0;}
 char *buffer=NULL;size_t length=0;
 FILE *memory=open_memstream(&buffer,&length);
 if(!memory){bf_json_free(doc);return 0;}
 BfTpl context={.out=memory,.max=max_output,.doc=doc,.root=root,.scope=root};
 const mini_mustach_itf_t itf={
   .emit=bf_tmpl_emit,.get=bf_tmpl_get,.enter=bf_tmpl_enter,
   .next=bf_tmpl_next,.leave=bf_tmpl_leave,.partial=NULL
 };
 int result=mini_mustach(trusted_template,0,&itf,&context);
 if(fclose(memory)!=0)result=MUSTACH_ERROR_SYSTEM;
 bf_json_free(doc);
 if(result!=MUSTACH_OK){free(buffer);return 0;}
 int ok=length<=max_output&&fwrite(buffer,1,length,out)==length;
 free(buffer);return ok;
}
