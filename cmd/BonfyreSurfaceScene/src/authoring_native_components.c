/* Native C/Wasm-ready authoring presentation. GrapesJS/Puck/Editor.js share
 * the typed structure; 43 fixed Mustach templates are generated from the
 * first-party catalogue. No editor HTML, template, script, CSS, identity, or
 * publication decision is accepted by this renderer. */
#define _GNU_SOURCE
#include "authoring_native_components.h"
#include "authoring_template.h"
#include "authoring_editorial.h"
#include "../../BonfyreWebAddress/generated/native_authoring_templates.h"
#include "bf_json.h"
#include "bf_jsonw.h"
#include <ctype.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define BF_NATIVE_PAGE_LIMIT (1024u*1024u)
typedef struct {
 FILE *stream;
 size_t written,limit,visited;
 const BfAuthoringTree *tree;
} BfNativePage;

static int is_reserved(const char *key,size_t size){
 static const char *const deny[]={
  "__proto__","prototype","constructor","html","script","style",
  "innerHTML","outerHTML","dangerouslySetInnerHTML"
 };
 if(size>=3&&!memcmp(key,"ak_",3))return 1;
 if(size>2&&key[0]=='o'&&key[1]=='n'&&key[2]>='A'&&key[2]<='Z')return 1;
 for(size_t i=0;i<sizeof(deny)/sizeof(*deny);i++)
  if(strlen(deny[i])==size&&!memcmp(deny[i],key,size))return 1;
 return 0;
}
static int key_is(const bf_json_node_t *n,const char *name){
 return n&&n->key&&strlen(name)==(size_t)n->key_len&&!memcmp(n->key,name,(size_t)n->key_len);
}
static int safe_link(const char *s){
 size_t n=s?strlen(s):0;
 if(!s||n>2000)return 0;
 if(!n)return 1;
 for(size_t i=0;i<n;i++){
  unsigned char c=(unsigned char)s[i];
  if(c<33||c==127||c=='<'||c=='>'||c=='"'||c==39||c=='\\')return 0;
 }
 if(s[0]=='#'){
  if(n<2||!isalpha((unsigned char)s[1]))return 0;
  for(size_t i=2;i<n;i++)if(!isalnum((unsigned char)s[i])&&s[i]!='-'&&s[i]!='_')return 0;
  return 1;
 }
 if(s[0]=='/'){
  if(s[1]=='/'||strstr(s,".."))return 0;
  return 1;
 }
 if(strncmp(s,"https://",8))return 0;
 const char *domain=s+8,*end=strpbrk(domain,"/?#"),*stop=end?end:s+n;
 if(stop<=domain||memchr(domain,'@',(size_t)(stop-domain)))return 0;
 int dot=0;for(const char *p=domain;p<stop;p++){
  if(*p=='.')dot=1;
  if(!isalnum((unsigned char)*p)&&*p!='.'&&*p!='-'&&*p!=':')return 0;
 }
 return dot;
}
static int safe_type(const char *value){
 static const char *const allowed[]={
  "text","email","tel","number","url","date","password",
  "textarea","select","checkbox","radio"
 };
 for(size_t i=0;i<sizeof(allowed)/sizeof(*allowed);i++)
  if(!strcmp(value,allowed[i]))return 1;
 return 0;
}
/* Validate every JSON string and key before it reaches a template attribute. */
static int safe_material(const bf_json_doc_t *doc,const bf_json_node_t *node,int depth){
 if(!node||depth>20)return 0;
 if(node->key){
  if(node->key_len<=0||node->key_len>80||
     is_reserved(node->key,(size_t)node->key_len))return 0;
 }
 if(node->type==BF_JSON_STRING){
  int n=0;const char *raw=bf_json_get_str(node,&n);
  if(!raw||n<0||n>32000)return 0;
  size_t bytes=0;char *value=bf_json_unescape(raw,n,&bytes);
  if(!value||bytes!=(size_t)strlen(value)){free(value);return 0;}
  int allowed=1;
  if(key_is(node,"href")||key_is(node,"src")||key_is(node,"url")||
     key_is(node,"image_url")||key_is(node,"origin"))allowed=safe_link(value);
  if(key_is(node,"type"))allowed=allowed&&safe_type(value);
  if(key_is(node,"name")){
   if(bytes>64)allowed=0;
   for(size_t i=0;i<bytes;i++)if(!isalnum((unsigned char)value[i])&&
      value[i]!='-'&&value[i]!='_')allowed=0;
  }
  if(strstr(value,"javascript:")||strstr(value,"<script")||
     strstr(value,"onerror=")||strstr(value,"onclick="))allowed=0;
  free(value);return allowed;
 }
 if(node->type==BF_JSON_ARRAY||node->type==BF_JSON_OBJECT){
  if(node->child_count>512)return 0;
  for(const bf_json_node_t *child=bf_json_child_first(doc,node);child;
      child=bf_json_child_next(doc,child))
   if(!safe_material(doc,child,depth+1))return 0;
 }
 return 1;
}
static int node_type_is(const bf_json_doc_t *doc,const bf_json_node_t *node,const char *name){
 const bf_json_node_t *type=bf_json_obj_get(doc,node,"type");
 if(!type||type->type!=BF_JSON_STRING)return 0;
 int len=0;const char *raw=bf_json_get_str(type,&len);
 return raw&&len==(int)strlen(name)&&!memcmp(raw,name,(size_t)len);
}
/* Index metadata belongs only to generated render context, never to CMS.
 * Mustach's iterator otherwise cannot generate tab/slide pair identifiers. */
static int enrich(bf_jsonw *out,const bf_json_doc_t *doc,
                  const bf_json_node_t *node,int depth,int index,int total){
 if(depth>20||!node)return 0;
 if(node->type==BF_JSON_OBJECT){
  bf_jsonw_obj(out);
  if(index>=0){
   bf_jsonw_key(out,"ak_index");bf_jsonw_int(out,index);
   bf_jsonw_key(out,"ak_number");bf_jsonw_int(out,index+1);
   bf_jsonw_key(out,"ak_total");bf_jsonw_int(out,total);
   bf_jsonw_key(out,"ak_first");bf_jsonw_bool(out,index==0);
   bf_jsonw_key(out,"ak_tabindex");bf_jsonw_int(out,index==0?0:-1);
  }
  if(bf_json_obj_get(doc,node,"type")){
   bf_jsonw_key(out,"ak_textarea");bf_jsonw_bool(out,node_type_is(doc,node,"textarea"));
   bf_jsonw_key(out,"ak_select");bf_jsonw_bool(out,node_type_is(doc,node,"select"));
   bf_jsonw_key(out,"ak_regular");bf_jsonw_bool(out,!node_type_is(doc,node,"textarea")&&
      !node_type_is(doc,node,"select"));
  }
  for(const bf_json_node_t *child=bf_json_child_first(doc,node);child;
      child=bf_json_child_next(doc,child)){
   if(!child->key||child->key_len<=0||child->key_len>80)return 0;
   bf_jsonw_keyn(out,child->key,(size_t)child->key_len);
   if(!enrich(out,doc,child,depth+1,-1,0))return 0;
  }
  bf_jsonw_obj_end(out);return !out->oom;
 }
 if(node->type==BF_JSON_ARRAY){
  bf_jsonw_arr(out);int i=0;
  for(const bf_json_node_t *child=bf_json_child_first(doc,node);child;
      child=bf_json_child_next(doc,child),i++)
   if(!enrich(out,doc,child,depth+1,i,node->child_count))return 0;
  bf_jsonw_arr_end(out);return !out->oom;
 }
 bf_jsonw_node(out,doc,node,0);return !out->oom;
}
static char *template_data(const BfAuthoringNode *node){
 const char *data=node->payload?node->payload:"{}";
 if(strlen(data)>90000)return NULL;
 bf_json_doc_t *doc=bf_json_parse_str(data,NULL,0);
 if(!doc)return NULL;
 const bf_json_node_t *root=bf_json_root(doc);
 if(!root||root->type!=BF_JSON_OBJECT||bf_json_count(doc)>2500||
    !safe_material(doc,root,0)){bf_json_free(doc);return NULL;}
 bf_jsonw w;bf_jsonw_init(&w,0);bf_jsonw_obj(&w);
 bf_jsonw_key(&w,"id");bf_jsonw_str(&w,node->id);
 bf_jsonw_key(&w,"kind");bf_jsonw_str(&w,node->kind);
 bf_jsonw_key(&w,"heading");bf_jsonw_str(&w,node->heading?node->heading:"");
 bf_jsonw_key(&w,"body");bf_jsonw_str(&w,node->editorial?"":node->body?node->body:"");
 bf_jsonw_key(&w,"payload");
 int ok=enrich(&w,doc,root,0,-1,0);
 bf_jsonw_obj_end(&w);bf_json_free(doc);
 char *result=NULL;
 if(ok&&!w.oom&&w.buf&&w.len<=220000){
  result=(char *)malloc(w.len+1);
  if(result)memcpy(result,w.buf,w.len+1);
 }
 bf_jsonw_free(&w);return result;
}
static int put(BfNativePage *page,const char *text,size_t size){
 if(!page||!text||size>page->limit||page->written>page->limit-size)return 0;
 if(size&&fwrite(text,1,size,page->stream)!=size)return 0;
 page->written+=size;return 1;
}
static int expand(BfNativePage *page,const char *template_text,const char *json){
 if(!template_text||!*template_text)return 1;
 char *output=NULL;size_t bytes=0;
 FILE *memory=open_memstream(&output,&bytes);
 if(!memory)return 0;
 const size_t remaining=page->limit-page->written;
 int ok=remaining&&bf_authoring_template_render(memory,template_text,json,remaining);
 if(fclose(memory))ok=0;
 if(ok)ok=put(page,output,bytes);
 free(output);return ok;
}
/* Render a compiled template and attach responsive semantics to its first
 * element. Layout values come from the generated native tree's bounded enum;
 * no authored CSS, selector, HTML or JS is accepted. */
static int expand_viewport(BfNativePage *page,const char *trusted_template,
                           const char *json,const BfAuthoringNode *node){
 static const char *const devices[]={"desktop","tablet","phone"};
 static const char *const layouts[]={"stack","grid","columns","carousel",
  "masonry","split","full-bleed","feature"};
 if(!node->viewport_columns[0]&&!node->viewport_columns[1]&&
    !node->viewport_columns[2])return expand(page,trusted_template,json);
 char *output=NULL;size_t bytes=0;
 FILE *memory=open_memstream(&output,&bytes);
 if(!memory)return 0;
 const size_t remaining=page->limit-page->written;
 int ok=remaining&&bf_authoring_template_render(memory,trusted_template,json,remaining);
 if(fclose(memory))ok=0;
 const char *end=ok&&output?memchr(output,'>',bytes):NULL;
 if(!end||output[0]!='<')ok=0;
 if(ok)ok=put(page,output,(size_t)(end-output));
 for(int i=0;ok&&i<3;i++){
  if(!node->viewport_columns[i])continue;
  const unsigned layout=node->viewport_layout[i],columns=node->viewport_columns[i];
  if(layout>=8||columns<1||columns>12){ok=0;break;}
  char attr[145];
  int n=snprintf(attr,sizeof(attr)," data-%s-layout=\"%s\" data-%s-columns=\"%u\"",
   devices[i],layouts[layout],devices[i],columns);
  if(n<0||(size_t)n>=sizeof(attr)){ok=0;break;}
  ok=put(page,attr,(size_t)n);
 }
 if(ok)ok=put(page,end,bytes-(size_t)(end-output));
 free(output);return ok;
}
static int render_children(BfNativePage *page,const char *parent,int depth){
 if(depth>BF_AUTHORING_TREE_DEPTH)return 0;
 const BfAuthoringTree *tree=page->tree;
 const int count=bf_authoring_tree_nchildren(tree,parent);
 if(count<0||count>BF_AUTHORING_TREE_MAX)return 0;
 for(int position=0;position<count;position++){
  const BfAuthoringNode *node=NULL;
  for(size_t i=0;i<tree->count;i++)
   if(!strcmp(tree->nodes[i].parent,parent?parent:"")&&
      tree->nodes[i].position==(unsigned)position){node=&tree->nodes[i];break;}
  if(!node||++page->visited>tree->count)return 0;
  const BfNativeAuthoringTemplate *template=bf_native_authoring_template(node->kind);
  const BfAuthoringKind *kind=bf_authoring_tree_kind(node->kind);
  if(!template||!kind||(!kind->can_contain&&
     bf_authoring_tree_nchildren(tree,node->id)>0))return 0;
  char *json=template_data(node);
  if(!json)return 0;
  int ok=expand_viewport(page,template->prefix,json,node);
  if(ok&&node->editorial){
   /* Editor.js tool material is rendered only inside a valid editorial
    * container. All markup is bounded, first-party and escaped. */
   if(strcmp(node->kind,"profile-story")&&strcmp(node->kind,"profile-section"))ok=0;
   else {
    char *content=NULL;size_t bytes=0;
    FILE *memory=open_memstream(&content,&bytes);
    if(!memory)ok=0;
    else {
     const size_t remaining=page->limit-page->written;
     ok=remaining&&bf_authoring_editorial_render(memory,node->editorial,remaining);
     if(fclose(memory))ok=0;
     if(ok)ok=put(page,content,bytes);
    }
    free(content);
   }
  }
  if(ok)ok=render_children(page,node->id,depth+1);
  if(ok)ok=expand(page,template->suffix,json);
  free(json);
  if(!ok)return 0;
 }
 return 1;
}
int bf_authoring_native_render(FILE *out,const BfAuthoringTree *tree,size_t max_output){
 if(!out||!tree||tree->count>BF_AUTHORING_TREE_MAX||
    max_output<64||max_output>BF_NATIVE_PAGE_LIMIT)return 0;
 char *buffer=NULL;size_t bytes=0;
 FILE *memory=open_memstream(&buffer,&bytes);
 if(!memory)return 0;
 BfNativePage page={.stream=memory,.written=0,.limit=max_output,.tree=tree};
 static const char opening[]="<div class=\"authored-site\" data-authored-native=\"c\">";
 static const char closing[]="</div>";
 int ok=put(&page,opening,sizeof(opening)-1)&&
        render_children(&page,"",0)&&
        page.visited==tree->count&&
        put(&page,closing,sizeof(closing)-1);
 if(fclose(memory))ok=0;
 if(ok)ok=bytes<=max_output&&fwrite(buffer,1,bytes,out)==bytes;
 free(buffer);return ok;
}
