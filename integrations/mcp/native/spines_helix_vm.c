/* Same R6 Helix evaluator as installed native-spines-r7/package/native/helix_vm.c.
 * Only SHA provider uses Bonfyre portable SHA-256 instead of platform crypto. */
#include "spines_sha256_compat.h"
#include <ctype.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define VERSION "5.6.0-reasoning-r6"
typedef struct { const char *s; size_t i,n; int fuel,steps,error; } Parser;

static void skip(Parser *p){while(p->i<p->n && isspace((unsigned char)p->s[p->i]))p->i++;}
static int ident(Parser *p,char *out,size_t cap){
    skip(p);size_t j=0;
    while(p->i<p->n && (isalpha((unsigned char)p->s[p->i])||p->s[p->i]=='_'||isdigit((unsigned char)p->s[p->i]))){
        if(j+1<cap) out[j++]=p->s[p->i];
        p->i++;
    } out[j]=0;return j>0;
}
static double expr(Parser *p);
static int consume(Parser*p,char c){skip(p);if(p->i<p->n&&p->s[p->i]==c){p->i++;return 1;}p->error=1;return 0;}
static double expr(Parser *p){
    if(p->error||p->fuel<=0){p->error=1;return 0;} p->fuel--;p->steps++;skip(p);
    if(p->i<p->n && (isdigit((unsigned char)p->s[p->i])||p->s[p->i]=='-'||p->s[p->i]=='.')){
        char *end=NULL;double v=strtod(p->s+p->i,&end);if(end==p->s+p->i){p->error=1;return 0;}p->i=(size_t)(end-p->s);return v;
    }
    char op[64];if(!ident(p,op,sizeof(op))){p->error=1;return 0;}
    if(strcmp(op,"true")==0) return 1;
    if(strcmp(op,"false")==0) return 0;
    if(!consume(p,'('))return 0;
    double a=expr(p),b=0;
    int unary=(strcmp(op,"not")==0||strcmp(op,"neg")==0||strcmp(op,"sqrt")==0);
    if(!unary){if(!consume(p,','))return 0;b=expr(p);}
    if(!consume(p,')'))return 0;
    if(strcmp(op,"add")==0)return a+b;
    if(strcmp(op,"sub")==0)return a-b;
    if(strcmp(op,"mul")==0)return a*b;
    if(strcmp(op,"div")==0){if(b==0){p->error=1;return 0;}return a/b;}
    if(strcmp(op,"min")==0)return a<b?a:b;
    if(strcmp(op,"max")==0)return a>b?a:b;
    if(strcmp(op,"eq")==0)return fabs(a-b)<1e-12;
    if(strcmp(op,"lt")==0)return a<b;
    if(strcmp(op,"gt")==0)return a>b;
    if(strcmp(op,"and")==0)return (a!=0)&&(b!=0);
    if(strcmp(op,"or")==0)return (a!=0)||(b!=0);
    if(strcmp(op,"not")==0)return !(a!=0);
    if(strcmp(op,"neg")==0)return -a;
    if(strcmp(op,"sqrt")==0){if(a<0){p->error=1;return 0;}return sqrt(a);}
    p->error=1;return 0;
}
static const char *arg_value(int argc,char **argv,const char*name){for(int i=0;i<argc-1;i++)if(strcmp(argv[i],name)==0)return argv[i+1];return NULL;}
int main(int argc,char **argv){
    if(argc<2)return 2;
    if(strcmp(argv[1],"status")==0||strcmp(argv[1],"--version")==0){
        printf("{\"status\":\"ok\",\"service\":\"bonfyre-helix-vm\",\"version\":\"%s\",\"effect_scope\":\"PURE\"}\n",VERSION);return 0;
    }
    if(strcmp(argv[1],"reduce")!=0)return 2;
    const char *term=arg_value(argc,argv,"--term"),*fuel_s=arg_value(argc,argv,"--fuel");
    if(!term) return 2;
    int fuel=fuel_s?atoi(fuel_s):10000;
    if(fuel<=0) fuel=1;
    Parser p={term,0,strlen(term),fuel,0,0};double value=expr(&p);skip(&p);
    if(p.i!=p.n)p.error=1;
    unsigned char d[32];char hex[65];bf_sha256((const unsigned char*)term,strlen(term),d);bf_hex(d,32,hex);
    if(p.error){printf("{\"schema\":\"bonfyre.helix-witness.v1\",\"state\":\"failed\",\"term_sha256\":\"%s\",\"steps\":%d,\"fuel_remaining\":%d}\n",hex,p.steps,p.fuel);return 3;}
    printf("{\"schema\":\"bonfyre.helix-witness.v1\",\"state\":\"passed\",\"term_sha256\":\"%s\",\"steps\":%d,\"fuel_remaining\":%d,\"result\":%.17g}\n",hex,p.steps,p.fuel,value);return 0;
}
