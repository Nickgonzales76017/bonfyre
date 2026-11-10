#include "bridge_project.h"
#include "../../../lib/libbonfyre/include/bf_command_lookup.h"
static int bridge_main(int argc,char**argv){const char*in=barg(argc,argv,"--input"),*out=barg(argc,argv,"--out"),*lr=barg(argc,argv,"--limit");DIR*d=NULL;struct dirent*e;sqlite3*j=NULL;int limit=lr?atoi(lr):256,seen=0,projected=0,rejected=0;char path[1200];
 enum{BD_TRANSLATE};static const BfCommandEntry bridge_commands[]={{"translate",BD_TRANSLATE,0,NULL},};const BfCommandEntry*command=argc<2?NULL:bf_command_lookup(bridge_commands,sizeof(bridge_commands)/sizeof(bridge_commands[0]),argv[1]);
 if(argc<2||!command||command->id!=BD_TRANSLATE||!in||!out){fputs("usage: bonfyre-pack-condition-delta translate --input CONDITION_DELTAS --out FACT_DELTAS [--limit N]\n",stderr);return 2;}
 if(limit<1)limit=1;
 if(limit>4096)limit=4096;
 if(!bdir(out)||!(d=opendir(in))||sqlite3_open(":memory:",&j)!=SQLITE_OK)return 1;
 while((e=readdir(d))&&seen<limit){size_t z=strlen(e->d_name);if(z<6||strcmp(e->d_name+z-5,".json"))continue;if(snprintf(path,sizeof(path),"%s/%s",in,e->d_name)>=(int)sizeof(path))continue;seen++;int r=bproject(j,path,out);if(r>0)projected++;else rejected++;}
 closedir(d);sqlite3_close(j);printf("{\"condition_notifications_seen\":%d,\"fact_deltas_projected\":%d,\"rejected\":%d,\"truth\":\"projection_only_no_authority_or_observation\"}\n",seen,projected,rejected);return rejected?1:0;
}
