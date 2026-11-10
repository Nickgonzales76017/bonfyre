#ifndef BONFYRE_REPOSITORY_CUT_RECEIPT_H
#define BONFYRE_REPOSITORY_CUT_RECEIPT_H
#include <bonfyre.h>
/* Preserve the exact virtual commit and tree. A JOSH rewrite is NOT an
 * upstream commit mapping and a source-cut digest is NOT a compiled pack. */
static int rf_rev_parse(const char *dest,const char *what,char hex[65]){
    int fd[2];if(pipe(fd))return 0;
    pid_t pid=fork();if(pid<0){close(fd[0]);close(fd[1]);return 0;}
    if(pid==0){
        close(fd[0]);if(dup2(fd[1],STDOUT_FILENO)<0)_exit(127);close(fd[1]);
        int null=open("/dev/null",O_WRONLY);
        if(null>=0){dup2(null,STDERR_FILENO);close(null);}
        execlp("git","git","-C",dest,"rev-parse","--verify",what,(char*)NULL);
        _exit(127);
    }
    close(fd[1]);char data[80];size_t n=0;ssize_t k;
    while(n<sizeof(data)-1&&(k=read(fd[0],data+n,sizeof(data)-1-n))>0)n+=(size_t)k;
    close(fd[0]);int status=0;
    if(waitpid(pid,&status,0)!=pid||!WIFEXITED(status)||WEXITSTATUS(status))return 0;
    while(n&&(data[n-1]=='\n'||data[n-1]=='\r'))n--;
    if(n!=40&&n!=64)return 0;
    for(size_t i=0;i<n;i++)if(!isxdigit((unsigned char)data[i]))return 0;
    memcpy(hex,data,n);hex[n]=0;return 1;
}
static int rf_cut_digest(const char *provider,const char *org,const char *repo,const char *ref,
                         const char *filter,const char *commit,const char *tree,char digest[65]){
    char data[1800];
    int n=snprintf(data,sizeof(data),"bonfyre.repository-cut.v1\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n",
        provider,org,repo,ref&&*ref?ref:"HEAD",filter&&*filter?filter:"",commit,tree);
    if(n<0||(size_t)n>=sizeof(data))return 0;
    bf_sha256_hex((const uint8_t*)data,(size_t)n,digest);return 1;
}
/* Opt-in immutable YAFF receipt, not an auto-published pack or source grant.
 * Exclusive creation and no symlink traversal prevent silent revision swaps. */
static int rf_cut_receipt(const char *path,const char *provider,const char *org,const char *repo,
                          const char *ref,const char *filter,const char *commit,const char *tree,
                          const char *digest){
    if(!path)return 1;
    if(*path!='/'||strlen(path)>=PATH_MAX||strchr(path,'\n'))return 0;
    char data[2048];
    int n=snprintf(data,sizeof(data),
        "schema: bonfyre.repository-cut.v1\n"
        "source_provider: %s\nsource_org: %s\nsource_repository: %s\n"
        "requested_ref: %s\njosh_filter: %s\nvirtual_commit: %s\nvirtual_tree: %s\n"
        "cut_digest: %s\ncanonical_mapping: %s\n"
        "pack_root: uncompiled\ninstall_authority: none\npublication_authority: none\n",
        provider,org,repo,ref&&*ref?ref:"HEAD",filter&&*filter?filter:"none",
        commit,tree,digest,filter&&*filter?"unverified":"direct-git");
    if(n<0||(size_t)n>=sizeof(data))return 0;
    int flags=O_WRONLY|O_CREAT|O_EXCL;
#ifdef O_NOFOLLOW
    flags|=O_NOFOLLOW;
#endif
    int fd=open(path,flags,0600);
    if(fd<0)return 0;
    size_t written=0;while(written<(size_t)n){
        ssize_t k=write(fd,data+written,(size_t)n-written);
        if(k<=0){close(fd);unlink(path);return 0;}written+=(size_t)k;
    }
    int ok=fsync(fd)==0;close(fd);if(!ok)unlink(path);
    return ok;
}
#endif
