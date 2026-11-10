#define _POSIX_C_SOURCE 200809L
#define _DARWIN_C_SOURCE 1
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sqlite3.h>
#include "../src/public_projection_cache.h"
static int checks;
static void check(int ok,const char *why){checks++;if(!ok){fprintf(stderr,"projection cache contention FAIL: %s\n",why);exit(1);}}
static double now(void){struct timespec t;clock_gettime(CLOCK_MONOTONIC,&t);return t.tv_sec*1000.0+t.tv_nsec/1000000.0;}
int main(void){
 char path[]="/tmp/public-cache-busy-XXXXXX";int fd=mkstemp(path);check(fd>=0,"fixture path");close(fd);
 const char *deps[]={"cms:fixture"};char digest[65];
 check(bf_public_projection_cache_write(path,"one","rev1","old-body",8,deps,1,digest)==1,"initial body cached");
 sqlite3 *lock=NULL;check(sqlite3_open(path,&lock)==SQLITE_OK,"lock connection");check(sqlite3_exec(lock,"BEGIN IMMEDIATE",NULL,NULL,NULL)==SQLITE_OK,"held competing writer");
 double start=now();check(bf_public_projection_cache_write(path,"two","rev2","new-body",8,deps,1,digest)==0,"contended optional write skipped");double elapsed=now()-start;
 check(elapsed<500,"optional write cannot hold response for the two-second busy timeout");
 FILE *out=tmpfile();check(out!=NULL,"capture");check(bf_public_projection_cache_read(path,"one","rev1",out)==1,"committed body readable during contention");
 check(bf_public_projection_cache_read(path,"two","rev2",out)==0,"no partial body from skipped write");
 check(sqlite3_exec(lock,"ROLLBACK",NULL,NULL,NULL)==SQLITE_OK,"release competing writer");sqlite3_close(lock);
 check(bf_public_projection_cache_write(path,"two","rev2","new-body",8,deps,1,digest)==1,"later write succeeds");
 check(bf_public_projection_cache_read(path,"two","wrong-revision",out)==0,"different revision never reused");
 check(bf_public_projection_cache_read(path,"two","rev2",out)==1,"exact new body read");
 int removed=0;check(bf_public_projection_cache_invalidate(path,"cms:fixture",&removed)==1&&removed==2,"invalidation still retires both dependencies");
 check(bf_public_projection_cache_read(path,"one","rev1",out)==0&&bf_public_projection_cache_read(path,"two","rev2",out)==0,"invalidated bodies stay absent");
 rewind(out);char text[17]={0};check(fread(text,1,16,out)==16&&!strcmp(text,"old-bodynew-body"),"read bytes unchanged");fclose(out);
 const char ok[]="HTTP/1.1 200 OK\r\n", error[]="HTTP/1.1 503 Unavailable\r\n", redirect[]="HTTP/1.1 302 Found\r\n", malformed[]="HTTP/1.1 2000 Bad\r\n";
 check(bf_public_projection_response_cacheable(ok,strlen(ok)),"successful response is cacheable");
 check(!bf_public_projection_response_cacheable(error,strlen(error))&&!bf_public_projection_response_cacheable(redirect,strlen(redirect))&&!bf_public_projection_response_cacheable(malformed,strlen(malformed))&&!bf_public_projection_response_cacheable(ok,8),"errors redirects and malformed status are not cacheable");
 /* Keep the 16.8 MiB streaming witness independent of scratch-disk capacity. */
 const size_t count=257,total=count*65536U;
 char *capture=malloc(total+1),*received=NULL;size_t received_bytes=0;
 check(capture!=NULL,"large capture allocation");
 FILE *large=fmemopen(capture,total+1,"w+"),*forwarded=open_memstream(&received,&received_bytes);
 check(large&&forwarded,"large memory capture fixture");
 char block[65536];for(size_t i=0;i<sizeof(block);i++)block[i]=(char)(i%251);
 for(int i=0;i<257;i++)check(fwrite(block,1,sizeof(block),large)==sizeof(block),"write large capture");
 check(bf_public_projection_forward(large,forwarded),"large captured response streams");
 check(fflush(forwarded)==0&&received_bytes==total,"large response length preserved");
 for(size_t i=0;i<count;i++)check(!memcmp(received+i*sizeof(block),block,sizeof(block)),"large response bytes preserved");
 fclose(large);fclose(forwarded);free(received);free(capture);
 unlink(path);char side[1024];snprintf(side,sizeof(side),"%s-wal",path);unlink(side);snprintf(side,sizeof(side),"%s-shm",path);unlink(side);
 printf("Public cache contention: %d assertions; skipped write %.1fms: PASS\n",checks,elapsed);return 0;
}
