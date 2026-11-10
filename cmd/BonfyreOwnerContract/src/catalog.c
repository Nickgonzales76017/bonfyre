#include "catalog.h"
#include <ctype.h>
#include <dirent.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif
#define MAX_ORGANS 512
#define MAX_CAPS 512

typedef struct { char id[128], name[128], norm[128]; } Organ;
typedef struct {
 char id[128],name[256],binary[256],source[64],stage[128],artifact[256];
 char norm_binary[256],realized_path[PATH_MAX];
 int organ_idx,group_size,primary,archive,realized,source_owner_count;
 int source_owner_idx[32];
} Cap;

static void norm(const char*in,char*out,size_t n){
 size_t j=0;
 for(size_t i=0;in&&in[i]&&j+1<n;i++) if(isalnum((unsigned char)in[i])) out[j++]=(char)tolower((unsigned char)in[i]);
 out[j]=0;
}
static int starts(const char*s,const char*p){return s&&p&&!strncmp(s,p,strlen(p));}
static int load_organs(const char*path,Organ*o,int*n){
 FILE*f=fopen(path,"r"); if(!f)return 0;
 char l[4096],current[128]=""; int k=0,in_arch=0,saw_arch=0;
 while(fgets(l,sizeof l,f)&&k<MAX_ORGANS){
  if(strstr(l,"\"architectures\": {")){in_arch=1;saw_arch=1;continue;}
  if(in_arch&&strstr(l,"\"interactions\": {"))break;
  if(in_arch){
   if(l[0]==' '&&l[1]==' '&&l[2]==' '&&l[3]==' '&&l[4]=='\"'){
    char*e=strstr(l+5,"\": {");
    if(e){size_t z=(size_t)(e-(l+5));if(z&&z<sizeof current){memcpy(current,l+5,z);current[z]=0;}}
   }
   if(*current&&strstr(l,"\"canonical_name\":")){
    char*p=strchr(l,':'); if(!p)continue; p++; while(*p==' '||*p=='\"')p++;
    size_t z=0; while(p[z]&&p[z]!='\"'&&p[z]!='\n'&&z+1<sizeof o[k].name)z++;
    if(!z)continue;
    snprintf(o[k].id,sizeof o[k].id,"%s",current);
    memcpy(o[k].name,p,z); o[k].name[z]=0;
    norm(o[k].name,o[k].norm,sizeof o[k].norm); k++; *current=0;
   }
   continue;
  }
  if(!(l[0]==' '&&l[1]==' '&&l[2]=='\"'))continue;
  char*e=strstr(l+3,"\": {"); if(!e)continue;
  size_t z=(size_t)(e-(l+3)); if(!z||z>=sizeof o[k].name)continue;
  memcpy(o[k].name,l+3,z); o[k].name[z]=0;
  snprintf(o[k].id,sizeof o[k].id,"%s",o[k].name);
  norm(o[k].name,o[k].norm,sizeof o[k].norm); k++;
 }
 fclose(f); *n=k; return saw_arch?k>0:1;
}
static void trim(char*s){
 char*p=s; while(*p&&isspace((unsigned char)*p))p++;
 if(p!=s)memmove(s,p,strlen(p)+1);
 size_t n=strlen(s); while(n&&isspace((unsigned char)s[n-1]))s[--n]=0;
}
static int show_cap(const char*capbin,const char*id,Cap*c){
 char cmd[PATH_MAX+256]; snprintf(cmd,sizeof cmd,"'%s' show '%s'",capbin,id);
 FILE*p=popen(cmd,"r"); if(!p)return 0;
 snprintf(c->id,sizeof c->id,"%s",id); char l[4096];
 while(fgets(l,sizeof l,p)){
  char*colon=strchr(l,':'); if(!colon)continue; *colon=0;
  char*value=colon+1; trim(l); trim(value);
  if(!strcmp(l,"name"))snprintf(c->name,sizeof c->name,"%s",value);
  else if(!strcmp(l,"binary"))snprintf(c->binary,sizeof c->binary,"%s",value);
  else if(!strcmp(l,"source"))snprintf(c->source,sizeof c->source,"%s",value);
  else if(!strcmp(l,"stage"))snprintf(c->stage,sizeof c->stage,"%s",value);
  else if(!strcmp(l,"artifact_out"))snprintf(c->artifact,sizeof c->artifact,"%s",value);
 }
 int rc=pclose(p);
 if(rc||!*c->binary)return 0;
 norm(c->binary,c->norm_binary,sizeof c->norm_binary); c->organ_idx=-1;
 c->archive=strstr(c->binary,".archive")||strstr(c->id,"selftest");
 return 1;
}
static int load_caps(const char*capbin,Cap*c,int*n){
 char cmd[PATH_MAX+32]; snprintf(cmd,sizeof cmd,"'%s' list",capbin);
 FILE*p=popen(cmd,"r"); if(!p)return 0;
 char l[4096],ids[MAX_CAPS][128]; int ni=0,header=0;
 while(fgets(l,sizeof l,p)&&ni<MAX_CAPS){
  if(!header){if(!strncmp(l,"ID",2))header=1;continue;}
  char id[128]={0}; if(sscanf(l,"%127s",id)!=1)continue;
  snprintf(ids[ni++],sizeof ids[ni],"%s",id);
 }
 int rc=pclose(p); if(rc||!header)return 0;
 int k=0; for(int i=0;i<ni;i++)if(show_cap(capbin,ids[i],&c[k]))k++;
 *n=k; return k==ni;
}
static int isexe(const char*p){return p&&*p&&access(p,X_OK)==0;}
static void realize(Cap*c,const char*bin){
 snprintf(c->realized_path,sizeof c->realized_path,"%s/%s",bin,c->binary);
 if(isexe(c->realized_path)){c->realized=1;return;}
 DIR*d=opendir(bin); if(!d){c->realized_path[0]=0;return;}
 struct dirent*de;
 while((de=readdir(d))){
  char n[256],p[PATH_MAX]; norm(de->d_name,n,sizeof n);
  if(strcmp(n,c->norm_binary))continue;
  snprintf(p,sizeof p,"%s/%s",bin,de->d_name);
  if(isexe(p)){snprintf(c->realized_path,sizeof c->realized_path,"%s",p);c->realized=1;break;}
 }
 closedir(d); if(!c->realized)c->realized_path[0]=0;
}
static int match_organ(const Cap*c,const Organ*o,int n){
 char nn[256]; norm(c->name,nn,sizeof nn);
 for(int i=0;i<n;i++)if(!strcmp(c->norm_binary,o[i].norm)||!strcmp(nn,o[i].norm))return i;
 const char*alias=0;
 if(!strcmp(c->id,"layer-c"))alias="bonfyrelayer";
 else if(!strcmp(c->id,"surface-ir"))alias="bonfyresurface";
 else if(!strcmp(c->id,"audio-ingest"))alias="bonfyreingest";
 if(alias)for(int i=0;i<n;i++)if(!strcmp(alias,o[i].norm))return i;
 return -1;
}
static int organ_by_id(const Organ*o,int n,const char*id){
 for(int i=0;i<n;i++)if(!strcmp(o[i].id,id))return i;
 return -1;
}
static void add_source_owner(Cap*c,int idx){
 if(idx<0)return;
 for(int i=0;i<c->source_owner_count;i++)if(c->source_owner_idx[i]==idx)return;
 if(c->source_owner_count<32)c->source_owner_idx[c->source_owner_count++]=idx;
}
static int scan_source_owners(const char*atlas,const Organ*o,int no,Cap*c,int nc){
 FILE*f=fopen(atlas,"r"); if(!f)return 0;
 char l[4096],current[128]=""; int in_arch=0,in_sources=0,current_idx=-1;
 while(fgets(l,sizeof l,f)){
  if(strstr(l,"\"architectures\": {")){in_arch=1;continue;}
  if(in_arch&&strstr(l,"\"interactions\": {"))break;
  if(!in_arch)continue;
  if(l[0]==' '&&l[1]==' '&&l[2]==' '&&l[3]==' '&&l[4]=='\"'){
   char*e=strstr(l+5,"\": {");
   if(e){
    size_t z=(size_t)(e-(l+5));
    if(z&&z<sizeof current){memcpy(current,l+5,z);current[z]=0;current_idx=organ_by_id(o,no,current);in_sources=0;}
   }
  }
  if(current_idx>=0&&strstr(l,"\"source_paths\": [")){in_sources=1;continue;}
  if(in_sources&&strchr(l,']')){in_sources=0;continue;}
  if(!in_sources||current_idx<0)continue;
  const char*family=strstr(l,"cmd/Bonfyre");
  if(!family)continue;
  family+=4;
  const char*slash=strchr(family,'/');
  if(!slash)continue;
  char family_raw[256],family_norm[256]; size_t fz=(size_t)(slash-family);
  if(!fz||fz>=sizeof family_raw)continue;
  memcpy(family_raw,family,fz);family_raw[fz]=0;norm(family_raw,family_norm,sizeof family_norm);
  for(int i=0;i<nc;i++){
   if(!c[i].realized||c[i].archive||!starts(c[i].norm_binary,"bonfyre"))continue;
   if(!strcmp(family_norm,c[i].norm_binary))add_source_owner(&c[i],current_idx);
  }
 }
 fclose(f); return 1;
}
static int primary_score(const Cap*c,const Organ*o){
 char idn[128],namen[256],base[128]; norm(c->id,idn,sizeof idn);norm(c->name,namen,sizeof namen);
 snprintf(base,sizeof base,"%s",o->norm); if(starts(base,"bonfyre"))memmove(base,base+7,strlen(base+7)+1);
 int s=0;
 if(c->realized&&strstr(c->realized_path,"/.bonfyre/bin/")&&strstr(c->realized_path,c->binary))s+=8;
 else if(c->realized)s+=4;
 if(!strcmp(idn,base))s+=4;
 if(!strcmp(namen,o->norm))s+=2;
 if(!strcmp(c->source,"probed")||!strcmp(c->source,"discovered"))s++;
 return s;
}

int catalog_summary(const char *atlas,const char *capbin,BfOwnerCatalogSummary *out){
 int no=0,nc=0;
 Organ organs[MAX_ORGANS]={0}; Cap caps[MAX_CAPS]={0};
 if(!atlas||!capbin||!out)return 0;
 memset(out,0,sizeof(*out));
 if(!load_organs(atlas,organs,&no))return 0;
 if(!load_caps(capbin,caps,&nc))return 0;
 const char*home=getenv("HOME"); char bindir[PATH_MAX]; snprintf(bindir,sizeof bindir,"%s/.bonfyre/bin",home?home:"");
 for(int i=0;i<nc;i++){realize(&caps[i],bindir);if(!caps[i].archive)caps[i].organ_idx=match_organ(&caps[i],organs,no);}
 if(!scan_source_owners(atlas,organs,no,caps,nc))return 0;
 for(int i=0;i<nc;i++)if(caps[i].organ_idx>=0){
  int best=i,bscore=primary_score(&caps[i],&organs[caps[i].organ_idx]),group=0;
  for(int j=0;j<nc;j++)if(caps[j].organ_idx==caps[i].organ_idx){group++;int s=primary_score(&caps[j],&organs[caps[j].organ_idx]);if(s>bscore){best=j;bscore=s;}}
  caps[i].group_size=group;if(best==i)caps[i].primary=1;
 }
 out->capability_rows=nc;out->atlas_architectures=no;
 for(int i=0;i<nc;i++){
  if(caps[i].archive)out->archives++;
  else if(caps[i].organ_idx>=0&&caps[i].realized&&caps[i].primary)out->canonical_realizations++;
  else if(caps[i].organ_idx>=0&&caps[i].realized)out->operation_aliases++;
  else if(!caps[i].realized)out->declared_unrealized++;
  else if(!starts(caps[i].norm_binary,"bonfyre"))out->external_realizations++;
  else if(caps[i].source_owner_count==1)out->source_linked_realizations++;
  else if(caps[i].source_owner_count>1)out->multi_owner_realizations++;
  else out->realized_unmapped++;
 }
 return 1;
}

int catalog_audit(int argc,char**argv){
 if(argc<4){fprintf(stderr,"usage: bonfyre-owner-contract audit-catalog ATLAS_INDEX CAPABILITY_BIN [--summary]\n");return 2;}
 int summary=argc>4&&!strcmp(argv[4],"--summary"),no=0,nc=0;
 Organ organs[MAX_ORGANS]={0}; Cap caps[MAX_CAPS]={0};
 if(!load_organs(argv[2],organs,&no)){perror(argv[2]);return 2;}
 if(!load_caps(argv[3],caps,&nc)){fprintf(stderr,"cannot read capability owner %s\n",argv[3]);return 2;}
 const char*home=getenv("HOME"); char bindir[PATH_MAX]; snprintf(bindir,sizeof bindir,"%s/.bonfyre/bin",home?home:"");
 for(int i=0;i<nc;i++){realize(&caps[i],bindir);if(!caps[i].archive)caps[i].organ_idx=match_organ(&caps[i],organs,no);}
 if(!scan_source_owners(argv[2],organs,no,caps,nc)){fprintf(stderr,"cannot scan Atlas source lineage %s\n",argv[2]);return 2;}
 for(int i=0;i<nc;i++)if(caps[i].organ_idx>=0){
  int best=i,bscore=primary_score(&caps[i],&organs[caps[i].organ_idx]),group=0;
  for(int j=0;j<nc;j++)if(caps[j].organ_idx==caps[i].organ_idx){group++;int s=primary_score(&caps[j],&organs[caps[j].organ_idx]);if(s>bscore){best=j;bscore=s;}}
  caps[i].group_size=group;if(best==i)caps[i].primary=1;
 }
 int canonical=0,aliases=0,archives=0,declared=0,external=0,source_linked=0,multi_owner=0,unmapped=0;
 for(int i=0;i<nc;i++){
  const char*status;
  if(caps[i].archive){status="archive";archives++;}
  else if(caps[i].organ_idx>=0&&caps[i].realized&&caps[i].primary){status="canonical_realization";canonical++;}
  else if(caps[i].organ_idx>=0&&caps[i].realized){status="operation_alias";aliases++;}
  else if(!caps[i].realized){status="declared_unrealized";declared++;}
  else if(!starts(caps[i].norm_binary,"bonfyre")){status="external_realization";external++;}
  else if(caps[i].source_owner_count==1){status="source_linked_realization";source_linked++;}
  else if(caps[i].source_owner_count>1){status="multi_owner_realization";multi_owner++;}
  else{status="realized_unmapped";unmapped++;}
  if(!summary)printf("{\"id\":\"%s\",\"binary\":\"%s\",\"status\":\"%s\",\"architecture\":\"%s\",\"organ\":\"%s\",\"semantic_owner_count\":%d,\"realized\":%s}\n",caps[i].id,caps[i].binary,status,caps[i].organ_idx>=0?organs[caps[i].organ_idx].id:"",caps[i].organ_idx>=0?organs[caps[i].organ_idx].name:"",caps[i].source_owner_count,caps[i].realized?"true":"false");
 }
 printf("{\"schema\":\"bonfyre-capability-normalization.v2\",\"capability_rows\":%d,\"atlas_architectures\":%d,\"canonical_realizations\":%d,\"operation_aliases\":%d,\"source_linked_realizations\":%d,\"multi_owner_realizations\":%d,\"archives\":%d,\"declared_unrealized\":%d,\"external_realizations\":%d,\"realized_unmapped\":%d}\n",nc,no,canonical,aliases,source_linked,multi_owner,archives,declared,external,unmapped);
 return unmapped?1:0;
}
