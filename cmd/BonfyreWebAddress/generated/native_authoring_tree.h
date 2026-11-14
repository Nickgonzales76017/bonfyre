/* GENERATED from Aurekai's shared editor tree grammar. Do not edit by hand.
 * Native C owns structural operations; browser frameworks own DOM interaction.
 * Validity is NOT account admission, source rights or publication approval. */
#ifndef BONFYRE_NATIVE_AUTHORING_TREE_H
#define BONFYRE_NATIVE_AUTHORING_TREE_H
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdio.h>
#include "bf_json.h"
#define BF_AUTHORING_TREE_MAX 512
#define BF_AUTHORING_TREE_DEPTH 16
typedef struct {const char *kind;int can_contain;} BfAuthoringKind;
static const BfAuthoringKind bf_authoring_tree_kinds[]={
 {"profile-hero",0},
 {"profile-section",1},
 {"profile-card",0},
 {"profile-story",1},
 {"profile-quote",0},
 {"profile-metric",0},
 {"profile-action",0},
 {"service:amounts",0},
 {"service:ask",0},
 {"service:calculator",0},
 {"service:call",0},
 {"service:cases",1},
 {"service:catalogue",0},
 {"service:compare",0},
 {"service:creator",0},
 {"service:door",0},
 {"service:figures",0},
 {"service:hero",1},
 {"service:library",0},
 {"service:listings",0},
 {"service:opportunity",0},
 {"service:person",0},
 {"service:plan",0},
 {"service:planner",0},
 {"service:pricing",0},
 {"service:proof",0},
 {"service:prose",1},
 {"service:reviews",0},
 {"service:schema",0},
 {"service:shipments",0},
 {"service:simulation",0},
 {"service:steps",0},
 {"service:synthesis",0},
 {"service:workbook",0},
 {"ui:section",1},
 {"ui:container",1},
 {"ui:columns",1},
 {"ui:grid",1},
 {"ui:stack",1},
 {"ui:hero",1},
 {"ui:card",1},
 {"ui:alert",0},
 {"ui:badge",0},
 {"ui:buttons",0},
 {"ui:button-group",0},
 {"ui:action",1},
 {"ui:choice",1},
 {"ui:field",0},
 {"ui:breadcrumb",0},
 {"ui:navbar",1},
 {"ui:nav-tabs",0},
 {"ui:scrollspy",1},
 {"ui:pagination",0},
 {"ui:accordion",0},
 {"ui:collapse",0},
 {"ui:dropdown",0},
 {"ui:modal",0},
 {"ui:offcanvas",0},
 {"ui:toast",0},
 {"ui:popover",0},
 {"ui:tooltip",0},
 {"ui:carousel",0},
 {"ui:progress",0},
 {"ui:spinner",0},
 {"ui:placeholder",0},
 {"ui:list-group",0},
 {"ui:table",0},
 {"ui:form",1},
 {"ui:input",0},
 {"ui:checks-radios",0},
 {"ui:select",0},
 {"ui:range",0},
 {"ui:documentation",1},
};
typedef struct {
 char id[119],parent[119],kind[64];
 unsigned position,readonly;
 char *heading,*body,*payload,*editorial;
 unsigned char viewport_layout[3];
 unsigned char viewport_columns[3];
 unsigned char viewport_gap[3];
} BfAuthoringNode;
typedef struct {BfAuthoringNode nodes[BF_AUTHORING_TREE_MAX];size_t count;} BfAuthoringTree;
static inline int bf_authoring_tree_id(const char *s) {
 size_t i,n;if(!s||(n=strlen(s))<1||n>118)return 0;
 for(i=0;i<n;i++)if(!((s[i]>='a'&&s[i]<='z')||
   (s[i]>='0'&&s[i]<='9')||(i&&s[i]=='-')))return 0;
 return 1;
}
static inline const BfAuthoringKind *bf_authoring_tree_kind(const char *kind){
 if(!kind)return NULL;
 for(size_t i=0;i<sizeof(bf_authoring_tree_kinds)/sizeof(bf_authoring_tree_kinds[0]);i++)
  if(!strcmp(kind,bf_authoring_tree_kinds[i].kind))return &bf_authoring_tree_kinds[i];
 return NULL;
}
static inline int bf_authoring_tree_index(const BfAuthoringTree *t,const char *id){
 if(!t||!id)return -1;
 for(size_t i=0;i<t->count;i++)if(!strcmp(t->nodes[i].id,id))return (int)i;
 return -1;
}
static inline void bf_authoring_tree_clear(BfAuthoringTree *t){
 if(!t)return;
 for(size_t i=0;i<t->count;i++){free(t->nodes[i].heading);free(t->nodes[i].body);free(t->nodes[i].payload);free(t->nodes[i].editorial);}
 memset(t,0,sizeof(*t));
}
static inline int bf_authoring_tree_nchildren(const BfAuthoringTree *t,const char *parent){
 size_t n=0;
 if(!t)return -1;
 for(size_t i=0;i<t->count;i++)
  if(!strcmp(t->nodes[i].parent,parent?parent:""))n++;
 return (int)n;
}
static inline int bf_authoring_tree_depth(const BfAuthoringTree *t,const char *id){
 int depth=0;
 while(id&&*id){
  int at=bf_authoring_tree_index(t,id);
  if(at<0||++depth>BF_AUTHORING_TREE_DEPTH+1)return -1;
  id=t->nodes[at].parent;
 }
 return depth;
}
static inline int bf_authoring_tree_contains(const BfAuthoringTree *t,const char *ancestor,const char *id){
 if(!ancestor||!*ancestor)return 0;
 while(id&&*id){
  if(!strcmp(id,ancestor))return 1;
  int i=bf_authoring_tree_index(t,id);
  if(i<0)return 0;
  id=t->nodes[i].parent;
 }
 return 0;
}
static inline int bf_authoring_tree_add(BfAuthoringTree *t,const char *id,const char *kind,const char *parent,int at){
 if(!t||t->count>=BF_AUTHORING_TREE_MAX||!bf_authoring_tree_id(id)||
    bf_authoring_tree_index(t,id)>=0)return 0;
 const BfAuthoringKind *k=bf_authoring_tree_kind(kind);
 if(!k)return 0;
 if(parent&&*parent){
  int p=bf_authoring_tree_index(t,parent);
  if(p<0||!bf_authoring_tree_kind(t->nodes[p].kind)->can_contain||
     bf_authoring_tree_depth(t,parent)>=BF_AUTHORING_TREE_DEPTH)return 0;
 }
 const char *scope=parent?parent:"";
 int children=bf_authoring_tree_nchildren(t,scope);
 if(at<0)at=children;
 if(at>children)return 0;
 for(size_t i=0;i<t->count;i++)
  if(!strcmp(t->nodes[i].parent,scope)&&t->nodes[i].position>=(unsigned)at)
   t->nodes[i].position++;
 BfAuthoringNode *newnode=&t->nodes[t->count++];
 memset(newnode,0,sizeof(*newnode));
 strcpy(newnode->id,id);strcpy(newnode->kind,kind);
 if(*scope)strcpy(newnode->parent,scope);
 newnode->position=(unsigned)at;
 return 1;
}
static inline int bf_authoring_tree_move(BfAuthoringTree *t,const char *id,const char *parent,int at){
 int n=bf_authoring_tree_index(t,id);
 if(n<0||at<0)return 0;
 BfAuthoringNode *node=&t->nodes[n];
 const char *dest=parent?parent:"";
 if(*dest){
  int p=bf_authoring_tree_index(t,dest);
  if(p<0||!bf_authoring_tree_kind(t->nodes[p].kind)->can_contain||
     bf_authoring_tree_contains(t,id,dest)||
     bf_authoring_tree_depth(t,dest)>=BF_AUTHORING_TREE_DEPTH)return 0;
 }
 int existing=bf_authoring_tree_nchildren(t,dest);
 if(!strcmp(node->parent,dest))existing--;
 if(at>existing)return 0;
 char before[119];strcpy(before,node->parent);
 unsigned old=node->position;
 for(size_t i=0;i<t->count;i++){
  if((int)i==n)continue;
  BfAuthoringNode *s=&t->nodes[i];
  if(!strcmp(s->parent,before)&&s->position>old)s->position--;
 }
 for(size_t i=0;i<t->count;i++){
  if((int)i==n)continue;
  BfAuthoringNode *s=&t->nodes[i];
  if(!strcmp(s->parent,dest)&&s->position>=(unsigned)at)s->position++;
 }
 strcpy(node->parent,dest);node->position=(unsigned)at;
 return 1;
}
static inline void bf_authoring_tree_reindex(BfAuthoringTree *t){
 unsigned ranks[BF_AUTHORING_TREE_MAX];
 for(size_t i=0;i<t->count;i++){
  unsigned n=0;
  for(size_t j=0;j<t->count;j++)
   if(strcmp(t->nodes[i].parent,t->nodes[j].parent)==0&&
      (t->nodes[j].position<t->nodes[i].position||
       (t->nodes[j].position==t->nodes[i].position&&j<i)))n++;
  ranks[i]=n;
 }
 for(size_t i=0;i<t->count;i++)t->nodes[i].position=ranks[i];
}
static inline int bf_authoring_tree_remove(BfAuthoringTree *t,const char *id){
 if(bf_authoring_tree_index(t,id)<0)return 0;
 size_t write=0,old=t->count;
 for(size_t i=0;i<old;i++){
  BfAuthoringNode *n=&t->nodes[i];
  if(bf_authoring_tree_contains(t,id,n->id)){
   free(n->heading);free(n->body);free(n->payload);free(n->editorial);continue;
  }
  if(write!=i)t->nodes[write]=*n;
  write++;
 }
 t->count=write;
 for(size_t i=write;i<old;i++)memset(&t->nodes[i],0,sizeof(t->nodes[i]));
 bf_authoring_tree_reindex(t);
 return 1;
}
static inline char *bf_authoring_tree_copytext(const char *value,size_t max){
 if(!value)return NULL;
 size_t n=strlen(value);if(n>max)return NULL;
 char *p=(char *)malloc(n+1);
 if(p)memcpy(p,value,n+1);
 return p;
}
static inline int bf_authoring_tree_text(BfAuthoringTree *t,const char *id,const char *field,const char *value){
 int i=bf_authoring_tree_index(t,id);
 if(i<0||!field||!value||t->nodes[i].readonly)return 0;
 size_t max=0;char **target=NULL;
 if(!strcmp(field,"heading")){max=220;target=&t->nodes[i].heading;}
 else if(!strcmp(field,"body")){max=50000;target=&t->nodes[i].body;}
 else return 0;
 char *copy=bf_authoring_tree_copytext(value,max);
 if(!copy)return 0;
 free(*target);*target=copy;
 return 1;
}
/* Full domain records remain data. The existing native JSON parser validates
 * syntax and kinds before a bounded draft can be stored. No rendering,
 * source right, publication or effect is authorized by this call. */
static inline int bf_authoring_tree_data(BfAuthoringTree *t,const char *id,const char *field,const char *json){
 int i=bf_authoring_tree_index(t,id);
 if(i<0||!json||!field||t->nodes[i].readonly)return 0;
 int is_payload=!strcmp(field,"payload");
 if(!is_payload&&strcmp(field,"editorial"))return 0;
 size_t len=strlen(json),max=is_payload?90000:100000;
 if(len<2||len>max)return 0;
 bf_json_doc_t *doc=bf_json_parse(json,len,NULL,0);
 if(!doc)return 0;
 const bf_json_node_t *root=bf_json_root(doc);
 int valid=root&&root->type==(is_payload?BF_JSON_OBJECT:BF_JSON_ARRAY)&&bf_json_count(doc)<=2500;
 for(int j=0;valid&&j<bf_json_count(doc);j++){
  const bf_json_node_t *node=bf_json_at(doc,j);
  if(!node) {valid=0;break;}
  if(node->key){
   const char *bad[]={"__proto__","prototype","constructor","script","html"};
   for(size_t k=0;k<sizeof(bad)/sizeof(*bad);k++)
    if(node->key_len==(int)strlen(bad[k])&&!memcmp(node->key,bad[k],(size_t)node->key_len)){valid=0;break;}
   /* Editor.js list style is not executable CSS. Retain exactly two values. */
   if(node->key_len==5&&!memcmp(node->key,"style",5)){
    if(is_payload||node->type!=BF_JSON_STRING){valid=0;break;}
    int sn=0;const char *sv=bf_json_get_str(node,&sn);
    if(!sv||!((sn==7&&!memcmp(sv,"ordered",7))||
               (sn==9&&!memcmp(sv,"unordered",9)))){valid=0;break;}
   }
  }
  if(node->type==BF_JSON_STRING){
   int slen=0;const char *value=bf_json_get_str(node,&slen);
   const char *deny[]={"javascript:","<script","onerror=","onclick="};
   if(!value||slen>32000){valid=0;break;}
   for(size_t k=0;k<sizeof(deny)/sizeof(*deny);k++){
    size_t n=strlen(deny[k]);
    for(int at=0;at+n<=(size_t)slen;at++)
     if(!memcmp(value+at,deny[k],n)){valid=0;break;}
    if(!valid)break;
   }
  }
 }
 bf_json_free(doc);
 if(!valid)return 0;
 size_t total=0;
 for(size_t j=0;j<t->count;j++){
  total+=t->nodes[j].heading?strlen(t->nodes[j].heading):0;
  total+=t->nodes[j].body?strlen(t->nodes[j].body):0;
  total+=t->nodes[j].payload?strlen(t->nodes[j].payload):0;
  total+=t->nodes[j].editorial?strlen(t->nodes[j].editorial):0;
 }
 char **slot=is_payload?&t->nodes[i].payload:&t->nodes[i].editorial;
 size_t old=*slot?strlen(*slot):0;
 if(total-old>220000||len>220000-(total-old))return 0;
 char *copy=bf_authoring_tree_copytext(json,max);
 if(!copy)return 0;
 free(*slot);*slot=copy;
 return 1;
}
static inline int bf_authoring_tree_style(BfAuthoringTree *t,const char *id,int device,int layout,int columns){
 int i=bf_authoring_tree_index(t,id);
 if(i<0||device<0||device>2||layout<0||layout>7||columns<1||columns>12||
    t->nodes[i].readonly)return 0;
 t->nodes[i].viewport_layout[device]=(unsigned char)layout;
 t->nodes[i].viewport_columns[device]=(unsigned char)columns;
 return 1;
}
static inline int bf_authoring_tree_gap(BfAuthoringTree *t,const char *id,int device,int gap){
 int i=bf_authoring_tree_index(t,id);
 if(i<0||device<0||device>2||gap<0||gap>64||t->nodes[i].readonly)return 0;
 t->nodes[i].viewport_gap[device]=(unsigned char)(gap+1);
 return 1;
}
static inline int bf_authoring_tree_copy(BfAuthoringTree *dest,const BfAuthoringTree *src){
 if(!src||!dest||dest==src||src->count>BF_AUTHORING_TREE_MAX)return 0;
 memset(dest,0,sizeof(*dest));
 for(size_t i=0;i<src->count;i++){
  dest->nodes[i]=src->nodes[i];dest->nodes[i].heading=NULL;dest->nodes[i].body=NULL;dest->nodes[i].payload=NULL;dest->nodes[i].editorial=NULL;
  if(src->nodes[i].heading){
   dest->nodes[i].heading=bf_authoring_tree_copytext(src->nodes[i].heading,220);
   if(!dest->nodes[i].heading){dest->count=i+1;bf_authoring_tree_clear(dest);return 0;}
  }
  if(src->nodes[i].body){
   dest->nodes[i].body=bf_authoring_tree_copytext(src->nodes[i].body,50000);
   if(!dest->nodes[i].body){dest->count=i+1;bf_authoring_tree_clear(dest);return 0;}
  }
  if(src->nodes[i].payload){
   dest->nodes[i].payload=bf_authoring_tree_copytext(src->nodes[i].payload,90000);
   if(!dest->nodes[i].payload){dest->count=i+1;bf_authoring_tree_clear(dest);return 0;}
  }
  if(src->nodes[i].editorial){
   dest->nodes[i].editorial=bf_authoring_tree_copytext(src->nodes[i].editorial,100000);
   if(!dest->nodes[i].editorial){dest->count=i+1;bf_authoring_tree_clear(dest);return 0;}
  }
  dest->count++;
 }
 return 1;
}
typedef int (*BfAuthoringVisit)(const BfAuthoringNode *node,int depth,void *user);
static inline int bf_authoring_tree_walk_at(const BfAuthoringTree *t,const char *parent,int depth,BfAuthoringVisit fn,void *user){
 int siblings=bf_authoring_tree_nchildren(t,parent);
 if(siblings<0||depth>BF_AUTHORING_TREE_DEPTH)return 0;
 for(int at=0;at<siblings;at++){
  const BfAuthoringNode *node=NULL;
  for(size_t i=0;i<t->count;i++)
   if(!strcmp(t->nodes[i].parent,parent?parent:"")&&t->nodes[i].position==(unsigned)at){
    node=&t->nodes[i];break;
   }
  if(!node||!fn(node,depth,user))return 0;
  if(!bf_authoring_tree_walk_at(t,node->id,depth+1,fn,user))return 0;
 }
 return 1;
}
static inline int bf_authoring_tree_walk(const BfAuthoringTree *t,BfAuthoringVisit fn,void *user){
 return t&&fn&&bf_authoring_tree_walk_at(t,"",0,fn,user);
}
#endif
