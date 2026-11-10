#define _POSIX_C_SOURCE 200809L
#include "bf_target_obligation.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
static const char *arg(int ac,char **av,const char *name){for(int i=2;i+1<ac;i++)if(!strcmp(av[i],name))return av[i+1];return NULL;}
static int atom(const char *s){if(!s||!*s)return 0;for(;*s;s++){unsigned char c=(unsigned char)*s;if(c<33||c>126||c=='"'||c=='\\')return 0;}return 1;}
static int run_attention(const char *bin,const char *db,const char *watch,const char *subject,const char *temperature,const char *signal,const char *detail){pid_t pid=fork();int status=0;if(pid<0)return 0;if(pid==0){if(signal&&*signal){char *const v[]={(char*)bin,(char*)"schedule",(char*)"--db",(char*)db,(char*)"--watch",(char*)watch,(char*)"--subject",(char*)subject,(char*)"--temperature",(char*)temperature,(char*)"--reheat-on",(char*)signal,(char*)"--detail",(char*)detail,NULL};execv(bin,v);}else{char *const v[]={(char*)bin,(char*)"schedule",(char*)"--db",(char*)db,(char*)"--watch",(char*)watch,(char*)"--subject",(char*)subject,(char*)"--temperature",(char*)temperature,(char*)"--detail",(char*)detail,NULL};execv(bin,v);}_exit(127);}return waitpid(pid,&status,0)==pid&&WIFEXITED(status)&&WEXITSTATUS(status)==0;}
int main(int ac,char **av){const char *tdb,*adb,*id,*bin,*temperature,*signal="";BfTargetObligation *owner=NULL;BfTargetObligationRecord r;char watch[192],reheat[192],detail[256];
if(ac<2||strcmp(av[1],"sync")){fprintf(stderr,"usage: bonfyre-target-attention sync --target-db FILE --attention-db FILE --target ID --attention-bin PATH\n");return 2;}
tdb=arg(ac,av,"--target-db");adb=arg(ac,av,"--attention-db");id=arg(ac,av,"--target");bin=arg(ac,av,"--attention-bin");if(!atom(tdb)||!atom(adb)||!atom(id)||!atom(bin))return 2;
if(bf_target_obligation_open(tdb,&owner,stderr)||bf_target_obligation_get(owner,id,&r)){if(owner)bf_target_obligation_close(owner);return 3;}bf_target_obligation_close(owner);snprintf(watch,sizeof(watch),"target-state:%.160s",r.target_id);snprintf(reheat,sizeof(reheat),"target-state-change:%.150s",r.target_id);snprintf(detail,sizeof(detail),"TargetState:%s",bf_target_obligation_state_name(r.state));
switch(r.state){case BF_TARGET_OPEN:temperature="hot";signal=reheat;break;case BF_TARGET_WAITING_EXTERNAL_PREDICATE:temperature="cool";signal=reheat;break;case BF_TARGET_PROVEN:case BF_TARGET_SUPERSEDED:temperature="cold";signal="";break;default:return 4;}
if(!run_attention(bin,adb,watch,r.target_id,temperature,signal,detail))return 5;
printf("{\"schema\":\"bonfyre-target-attention-projection.v1\",\"target_id\":\"%s\",\"target_state\":\"%s\",\"temperature\":\"%s\",\"reheat_on\":\"%s\",\"truth\":\"target_state_projects_attention_only_not_proof_work_authority_or_effect\"}\n",r.target_id,bf_target_obligation_state_name(r.state),temperature,signal);return 0;}
