#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../../BonfyreWebAddress/generated/native_authoring_tree.h"
static void ck(int ok,const char *name){
 if(!ok){fprintf(stderr,"native authoring tree: %s\n",name);exit(1);}
}
typedef struct {char sequence[4096];size_t count;} View;
static int visitor(const BfAuthoringNode *n,int depth,void *context){
 View *v=(View *)context;size_t len=strlen(n->id);
 ck(v->count+len+4<sizeof(v->sequence),"visit bound");
 v->sequence[v->count++]=(char)('0'+depth);
 v->sequence[v->count++]=':';
 memcpy(v->sequence+v->count,n->id,len);v->count+=len;
 v->sequence[v->count++]=',';v->sequence[v->count]='\0';return 1;
}
int main(void){
 BfAuthoringTree *t=calloc(1,sizeof(*t)),*other=calloc(1,sizeof(*other));
 BfAuthoringTree *copy=calloc(1,sizeof(*copy));
 ck(t&&other&&copy,"allocate");
 ck(bf_authoring_tree_add(t,"section-a","profile-section",NULL,0),"section");
 ck(bf_authoring_tree_add(t,"first","profile-card","section-a",0),"first child");
 ck(bf_authoring_tree_add(t,"second","profile-story","section-a",1),"second child");
 ck(bf_authoring_tree_add(t,"section-b","profile-section",NULL,1),"second section");
 ck(bf_authoring_tree_add(t,"quote","profile-quote","section-b",0),"quote");
 ck(!bf_authoring_tree_add(t,"first","profile-card",NULL,0),"duplicate refused");
 ck(!bf_authoring_tree_add(t,"unsafe/tag","profile-card",NULL,0),"unsafe id refused");
 ck(!bf_authoring_tree_add(t,"unrecognized","user-script",NULL,0),"unknown type refused");
 ck(!bf_authoring_tree_add(t,"grandchild","profile-card","first",0),"non-container denied");
 ck(bf_authoring_tree_add(t,"story-child","profile-card","second",0),"story container accepts child");
 ck(bf_authoring_tree_remove(t,"story-child"),"remove nested child");
 ck(!bf_authoring_tree_move(t,"section-a","first",0),"subtree cycle denied");
 ck(bf_authoring_tree_move(t,"quote","section-a",1),"cross parent move");
 ck(bf_authoring_tree_nchildren(t,"section-a")==3,"moved child count");
 ck(bf_authoring_tree_text(t,"first","heading","Retained <source>"),"write escaped text as data");
 ck(!bf_authoring_tree_text(t,"first","not-a-property","bad"),"property authority");
 ck(bf_authoring_tree_style(t,"first",2,1,1),"phone layout");
 ck(!bf_authoring_tree_style(t,"first",3,1,1),"unknown device");
 ck(!bf_authoring_tree_style(t,"first",2,1,13),"oversize columns");
 View v={0};ck(bf_authoring_tree_walk(t,visitor,&v),"walk");
 ck(strstr(v.sequence,"0:section-a,1:first,1:quote,1:second,0:section-b,")!=NULL,"stable nested order");
 ck(bf_authoring_tree_copy(copy,t),"snapshot copy");
 ck(!strcmp(copy->nodes[bf_authoring_tree_index(copy,"first")].heading,"Retained <source>"),"deep text copy");
 ck(bf_authoring_tree_text(copy,"first","heading","Different"),"copy updated");
 ck(!strcmp(t->nodes[bf_authoring_tree_index(t,"first")].heading,"Retained <source>"),"copy isolated");
 ck(bf_authoring_tree_add(other,"other","profile-card",NULL,0),"second account");
 ck(bf_authoring_tree_remove(t,"section-a"),"remove subtree");
 ck(t->count==1&&bf_authoring_tree_index(t,"section-b")==0,"subtree removed");
 ck(bf_authoring_tree_index(other,"other")==0,"second account isolated");
 // Non-subtree nodes interleaved with descendants must not overwrite
 // intermediate parent information while the deletion walk is in progress.
 BfAuthoringTree *mix=calloc(1,sizeof(*mix));ck(mix!=NULL,"interleaved allocation");
 ck(bf_authoring_tree_add(mix,"root-a","profile-section",NULL,0),"interleaved root");
 ck(bf_authoring_tree_add(mix,"other-one","profile-card",NULL,1),"unrelated first");
 ck(bf_authoring_tree_add(mix,"branch-c","profile-section","root-a",0),"nested section");
 ck(bf_authoring_tree_add(mix,"other-two","profile-card",NULL,2),"unrelated second");
 ck(bf_authoring_tree_add(mix,"other-three","profile-card",NULL,3),"unrelated third");
 ck(bf_authoring_tree_add(mix,"leaf-d","profile-card","branch-c",0),"late grandchild");
 ck(bf_authoring_tree_remove(mix,"root-a"),"remove interleaved subtree");
 ck(mix->count==3,"interleaved subtree count");
 ck(bf_authoring_tree_index(mix,"leaf-d")<0,"late descendant removed");
 ck(bf_authoring_tree_index(mix,"branch-c")<0,"intermediate removed");
 View m={0};ck(bf_authoring_tree_walk(mix,visitor,&m),"interleaved walk");
 ck(strstr(m.sequence,"other-one")&&strstr(m.sequence,"other-two")&&
    strstr(m.sequence,"other-three"),"retained unrelated roots");
 bf_authoring_tree_clear(mix);free(mix);
 bf_authoring_tree_clear(copy);bf_authoring_tree_clear(t);bf_authoring_tree_clear(other);
 free(copy);free(t);free(other);
 puts("native authoring tree PASS");return 0;
}
