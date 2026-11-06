#include <sqlite3.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
static const char *barg(int argc,char **argv,const char *name){for(int i=2;i+1<argc;i++)if(!strcmp(argv[i],name))return argv[i+1];return NULL;}
static char *bread(const char *path,size_t *n){FILE*f=fopen(path,"rb");char*b=NULL;long z;if(!f)return NULL;if(fseek(f,0,SEEK_END)||((z=ftell(f))<0)||fseek(f,0,SEEK_SET)){fclose(f);return NULL;}b=malloc((size_t)z+1);if(!b){fclose(f);return NULL;}if(z&&fread(b,1,(size_t)z,f)!=(size_t)z){free(b);fclose(f);return NULL;}b[z]=0;fclose(f);if(n)*n=(size_t)z;return b;}
static uint64_t bhash(const char*s){uint64_t h=1469598103934665603ULL;for(const unsigned char*p=(const unsigned char*)s;*p;p++){h^=*p;h*=1099511628211ULL;}return h;}
static int bdir(const char*p){return p&&*p&&(mkdir(p,0750)==0||errno==EEXIST);}
static int ball(int fd,const char*b,size_t n){while(n){ssize_t w=write(fd,b,n);if(w<0){if(errno==EINTR)continue;return 0;}b+=w;n-=(size_t)w;}return 1;}
static int bpublish(const char*dir,const char*delta,const char*json){char dest[1200],tmp[1240];int fd=-1,dfd=-1;if(!bdir(dir)||snprintf(dest,sizeof(dest),"%s/%016llx.json",dir,(unsigned long long)bhash(delta))>=(int)sizeof(dest)||snprintf(tmp,sizeof(tmp),"%s/.fact-delta-XXXXXX",dir)>=(int)sizeof(tmp)||(fd=mkstemp(tmp))<0)return 0;if(!ball(fd,json,strlen(json))||!ball(fd,"\n",1)||fsync(fd)){close(fd);unlink(tmp);return 0;}if(close(fd)||rename(tmp,dest)){unlink(tmp);return 0;}dfd=open(dir,O_RDONLY);if(dfd>=0){if(fsync(dfd)){close(dfd);return 0;}close(dfd);}return 1;}
