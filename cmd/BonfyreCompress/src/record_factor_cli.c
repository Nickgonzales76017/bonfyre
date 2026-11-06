#include "record_factor.h"
#include <errno.h>
#include <stdlib.h>
#include <string.h>
static void cell(const char *s){for(;s&&*s;s++){if(*s=='\t')fputs("\\t",stdout);else if(*s=='\n')fputs("\\n",stdout);else if(*s=='\r')fputs("\\r",stdout);else if(*s=='\\')fputs("\\\\",stdout);else putchar((unsigned char)*s);}}
static int print_row(void *c,sqlite3_int64 seq,const char *key,const char *title,const char *kind){
    (void)c;printf("%lld\t",(long long)seq);cell(key);putchar('\t');cell(title);putchar('\t');cell(kind);putchar('\n');return ferror(stdout)?1:0;
}
int bf_record_factor_command(int argc,char **argv){
    if(argc<2)return -1;
    const char *op=argv[1];
    if(strcmp(op,"factor-records")&&strcmp(op,"factor-export")&&strcmp(op,"factor-verify")&&strcmp(op,"factor-search")&&strcmp(op,"factor-read"))return -1;
    if(!strcmp(op,"factor-records")&&argc==4)return bf_record_factor_build(argv[2],argv[3],stdout);
    if(!strcmp(op,"factor-export")&&argc==3)return bf_record_factor_export(argv[2],stdout);
    if(!strcmp(op,"factor-verify")&&argc==4)return bf_record_factor_verify(argv[2],argv[3],stdout);
    if(!strcmp(op,"factor-search")&&(argc==3||argc==4)){
        sqlite3 *db=NULL;if(bf_record_factor_open(argv[2],&db))return 1;
        puts("ordinal\tkey\ttitle\tkind");int n=bf_record_factor_visit(db,argc==4?argv[3]:"",100,0,print_row,NULL);
        if(n<0)fprintf(stderr,"source search failed: %s\n",sqlite3_errmsg(db));
        sqlite3_close(db);return n<0?1:fflush(stdout)!=0;
    }
    if(!strcmp(op,"factor-read")&&argc==4){
        char *end=NULL;errno=0;long long seq=strtoll(argv[3],&end,10);
        if(errno||!end||*end||seq<1)return 2;
        sqlite3 *db=NULL;char *p=NULL;size_t n=0;if(bf_record_factor_open(argv[2],&db))return 1;
        int rc=bf_record_factor_read(db,(sqlite3_int64)seq,&p,&n);sqlite3_close(db);
        if(!rc)rc=fwrite(p,1,n,stdout)!=n||fflush(stdout)!=0;
        free(p);return rc;
    }
    fputs("factor-records SOURCE.jsonl NEW.sqlite\nfactor-search PACK.sqlite [QUERY]\nfactor-read PACK.sqlite ORDINAL\nfactor-export PACK.sqlite\nfactor-verify PACK.sqlite SOURCE.jsonl\n",stderr);
    return 2;
}
