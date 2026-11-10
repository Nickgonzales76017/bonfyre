#define _POSIX_C_SOURCE 200809L
#ifdef __APPLE__
#define _DARWIN_C_SOURCE
#endif
#include "../src/workspace_package.h"
int main(int argc,char **argv) {
    if(argc!=4)return 64;
    if(setenv("BONFYRE_WORKSPACE_PACKAGES",argv[1],1))return 65;
    size_t n=0;
    /* Exercise the ordinary path reader too: it must not open symlink members. */
    unsigned char *body=!strcmp(argv[2],"--file")?aw_read_file(argv[1],argv[3],AW_FILE,&n):aw_package(argv[2],argv[3],&n);
    if(!body)return 2;
    int failed=fwrite(body,1,n,stdout)!=n;free(body);return failed?3:0;
}
