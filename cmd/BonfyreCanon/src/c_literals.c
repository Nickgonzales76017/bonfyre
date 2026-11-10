#define _POSIX_C_SOURCE 200809L
#include "c_literals.h"
#include <bonfyre.h>
#include <ctype.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>
#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

static void je(FILE *f,const char *s,size_t n){
    for(size_t i=0;i<n;i++){unsigned char c=(unsigned char)s[i];
        if(c=='\"'||c=='\\'){fputc('\\',f);fputc(c,f);}
        else if(c=='\n')fputs("\\n",f);else if(c=='\r')fputs("\\r",f);
        else if(c=='\t')fputs("\\t",f);else if(c<0x20)fprintf(f,"\\u%04x",c);else fputc(c,f);}
}
static int loc(const char *s,int *ln,int *col){
    const char *p=strstr(s,"Loc=<"),*last,*prev;if(!p)return 0;
    last=strrchr(p,':');if(!last)return 0;prev=last;
    while(prev>p&&prev[-1]!=':')prev--;if(prev<=p)return 0;
    *ln=atoi(prev);*col=atoi(last+1);return *ln>0&&*col>0;
}
static int contentish(const char *s,size_t n){
    int a=0,sp=0;for(size_t i=0;i<n;i++){a+=isalpha((unsigned char)s[i])!=0;sp+=isspace((unsigned char)s[i])!=0;}
    return a>=4&&(sp>0||n>=12);
}
static char *decode_c_literal(const char *raw,size_t n,size_t *out_n){
    const char *q=NULL,*end=NULL;char *out;size_t w=0;
    for(size_t i=0;i<n;i++)if(raw[i]=='"'){q=raw+i;break;}
    if(q)for(size_t i=n;i>(size_t)(q-raw)+1;i--)if(raw[i-1]=='"'){end=raw+i-1;break;}
    if(!q||!end||end<=q){out=malloc(n+1);if(!out)return NULL;memcpy(out,raw,n);out[n]='\0';if(out_n)*out_n=n;return out;}
    out=malloc((size_t)(end-q)+1);if(!out)return NULL;
    for(const char *p=q+1;p<end;p++){
        unsigned char c=(unsigned char)*p;
        if(c!='\\'){out[w++]=(char)c;continue;}
        if(++p>=end){out[w++]='\\';break;}
        c=(unsigned char)*p;
        if(c=='n')out[w++]='\n';else if(c=='r')out[w++]='\r';else if(c=='t')out[w++]='\t';
        else if(c=='\\')out[w++]='\\';else if(c=='"')out[w++]='"';else if(c=='\'')out[w++]='\'';
        else if(c=='\n'){}else if(c=='\r'&&p+1<end&&p[1]=='\n')p++;
        else {out[w++]='\\';out[w++]=(char)c;}
    }
    out[w]='\0';if(out_n)*out_n=w;return out;
}
static int starts(const char *s,const char *prefix){return strncmp(s,prefix,strlen(prefix))==0;}
static const char *lexical_class(const char *s,size_t n){
    const char *p=s;int letters=0,spaces=0;(void)n;
    while(*p&&isspace((unsigned char)*p))p++;
    if(!*p)return "empty";
    if(starts(p,"HTTP/")||strstr(p,"Content-Type:")||strstr(p,"Content-Length:")||
       strstr(p,"Cache-Control:")||strstr(p,"Connection:"))return "http";
    if(starts(p,"SELECT ")||starts(p,"FROM ")||starts(p,"WHERE ")||starts(p,"JOIN ")||
       starts(p,"PRAGMA ")||starts(p,"ATTACH ")||strstr(p," FROM ")||strstr(p," WHERE ")||
       strstr(p,"INSERT ")||strstr(p,"UPDATE ")||strstr(p,"DELETE ")||
       strstr(p,"json_extract_string(")||strstr(p,"TRY_CAST(")||starts(p,"SET ")||
       starts(p,"INSTALL ")||starts(p,"LOAD ")||strstr(p," LIMIT ")||
       strstr(p," FILTER (")||strstr(p," AS DOUBLE")||strstr(p," AS BIGINT"))return "sql";
    if(strchr(p,'<')&&strchr(p,'>'))return "markup";
    if((strchr(p,'{')&&strchr(p,'}'))&&(starts(p,".")||starts(p,"#")||strchr(p,';')))return "css";
    if(strstr(p,"localStorage.")||strstr(p,"document.")||strstr(p,"=>")||
       starts(p,"const ")||starts(p,"let ")||starts(p,"var ")||strstr(p,"addEventListener("))return "javascript";
    if(*p=='"'){
        const char *e=p+strlen(p);
        while(e>p&&isspace((unsigned char)e[-1]))e--;
        if(e-p>=2&&e[-1]==','&&e[-2]=='"')return "json_fragment";
    }
    if(*p=='{'||*p=='['||starts(p,"\\\"")||
       ((*p=='"'||(*p==','&&p[1]=='"'))&&strstr(p,"\":")))return "json";
    if(starts(p,"application/")||starts(p,"text/")||starts(p,"image/")||
       starts(p,"audio/")||starts(p,"video/"))return "media_type";
    if(starts(p,"BONFYRE_")||starts(p,"--")||starts(p,"%s/")||*p=='/'||
       strstr(p,"://")||strstr(p,"/bonfyre-"))return "config_path_uri";
    for(const char *z=p;*z;z++){letters+=isalpha((unsigned char)*z)!=0;spaces+=isspace((unsigned char)*z)!=0;}
    if(spaces&&strchr(p,'-')){
        int css=1;for(const char *z=p;*z;z++)if(!(isalnum((unsigned char)*z)||*z=='-'||*z=='_'||isspace((unsigned char)*z))){css=0;break;}
        if(css)return "css_class";
    }
    if(!spaces){
        if(strchr(p,'_')||strchr(p,'.')||strchr(p,':')||strchr(p,'-'))return "identifier";
        for(const char *z=p+1;*z;z++)if(isupper((unsigned char)*z)&&islower((unsigned char)z[-1]))return "identifier";
    }
    if(strchr(p,'%'))return "format";
    if(letters>=4&&spaces>0)return "prose";
    if(letters>=4)return "label";
    return "technical";
}
static int site_fragment_class(const char *c){
    /* Format strings require typed runtime bindings and belong to the
     * SurfaceIR/template migration lane, not the plain authored-copy lane. */
    return !strcmp(c,"prose")||!strcmp(c,"label");
}
static FILE *clang_tokens(const char *clang,const char *file,pid_t *child){
    int fd[2];if(pipe(fd)!=0)return NULL;pid_t pid=fork();
    if(pid<0){close(fd[0]);close(fd[1]);return NULL;}
    if(pid==0){
        dup2(fd[1],STDERR_FILENO);close(fd[0]);close(fd[1]);
        char *const argv[]={(char *)clang,"-fsyntax-only","-Xclang","-dump-raw-tokens","--",(char *)file,NULL};
        execvp(clang,argv);_exit(127);
    }
    close(fd[1]);*child=pid;FILE *p=fdopen(fd[0],"r");
    if(!p){close(fd[0]);return NULL;}return p;
}
int bf_canon_c_literals(const char *file,const char *dir){
    char outp[PATH_MAX],candp[PATH_MAX],migp[PATH_MAX],dig[65],line[65536];const char *clang=getenv("BONFYRE_CLANG");
    FILE *p,*out,*cand,*mig;pid_t child=0;int count=0,candidates=0,site_candidates=0,status=0;
    if(!file||!dir||!*file||!*dir)return 2;if(!clang||!*clang)clang="clang";
    if(bf_ensure_dir(dir)!=0||bf_sha256_file(file,dig)!=0)return 1;
    if(snprintf(outp,sizeof(outp),"%s/literals.jsonl",dir)>=(int)sizeof(outp)||
       snprintf(candp,sizeof(candp),"%s/literal_candidates.jsonl",dir)>=(int)sizeof(candp)||
       snprintf(migp,sizeof(migp),"%s/site_fragment_migration.jsonl",dir)>=(int)sizeof(migp))return 2;
    out=fopen(outp,"w");cand=fopen(candp,"w");mig=fopen(migp,"w");
    if(!out||!cand||!mig){if(out)fclose(out);if(cand)fclose(cand);if(mig)fclose(mig);return 1;}
    fprintf(out,"{\"type\":\"bonfyre.c-literal-manifest.v2\",\"source\":\"");je(out,file,strlen(file));
    fprintf(out,"\",\"source_sha256\":\"%s\",\"lexer\":\"clang-dump-raw-tokens\"}\n",dig);
    p=clang_tokens(clang,file,&child);if(!p){fclose(out);fclose(cand);fclose(mig);return 1;}
    while(fgets(line,sizeof(line),p)){
        const char *k=line,*a,*b,*cls;char *decoded;size_t n,dn=0;int ln=0,col=0,candidate,site_candidate;
        while(*k&&isspace((unsigned char)*k))k++;
        if(strncmp(k,"string_literal",14))continue;a=strchr(k,'\'');if(!a)continue;
        b=strstr(a+1,"'\t");if(!b)b=strstr(a+1,"' ");if(!b||b<=a+1||!loc(line,&ln,&col))continue;
        n=(size_t)(b-a-1);decoded=decode_c_literal(a+1,n,&dn);if(!decoded){fclose(p);fclose(out);fclose(cand);fclose(mig);return 1;}
        candidate=contentish(decoded,dn);cls=lexical_class(decoded,dn);site_candidate=candidate&&site_fragment_class(cls);
        fprintf(out,"{\"type\":\"c_string_literal\",\"ordinal\":%d,\"line\":%d,\"column\":%d,\"source_ref\":\"",count,ln,col);
        je(out,file,strlen(file));fprintf(out,":%d:%d\",\"source_sha256\":\"%s\",\"content_candidate\":%s,\"lexical_class\":\"%s\",\"raw\":\"",ln,col,dig,candidate?"true":"false",cls);
        je(out,a+1,n);fputs("\",\"decoded\":\"",out);je(out,decoded,dn);fputs("\"}\n",out);
        if(candidate){
            fprintf(cand,"{\"type\":\"bonfyre.literal_candidate.v1\",\"source_ref\":\"");je(cand,file,strlen(file));
            fprintf(cand,":%d:%d\",\"source_line\":%d,\"source_column\":%d,\"source_ordinal\":%d,\"source_digest\":\"%s\",\"source_class\":\"c_literal\",\"lexical_class\":\"%s\",\"raw_literal\":\"",ln,col,ln,col,count,dig,cls);
            je(cand,a+1,n);fputs("\",\"extracted_text\":\"",cand);je(cand,decoded,dn);fputs("\"}\n",cand);candidates++;
        }
        if(site_candidate){
            fprintf(mig,"{\"candidate_key\":\"c:%.16s:%d:%d:%d\",\"source_ref\":\"",dig,ln,col,count);
            je(mig,file,strlen(file));fprintf(mig,":%d:%d\",\"source_line\":%d,\"source_column\":%d,\"source_ordinal\":%d,\"source_digest\":\"%s\",\"source_class\":\"c_literal\",\"lexical_class\":\"%s\",\"raw_literal\":\"",ln,col,ln,col,count,dig,cls);
            je(mig,a+1,n);fputs("\",\"extracted_text\":\"",mig);je(mig,decoded,dn);
            fputs("\",\"stage\":\"discovered\",\"compiled_fallback\":true,\"stale_source\":false}\n",mig);site_candidates++;
        }
        free(decoded);count++;
    }
    fclose(p);waitpid(child,&status,0);fclose(out);fclose(cand);fclose(mig);
    if(!WIFEXITED(status)||WEXITSTATUS(status)!=0)return 1;
    fprintf(stderr,"[canon] c-literals=%d lexical-candidates=%d site-fragment-candidates=%d source=%s -> %s + %s + %s\n",
            count,candidates,site_candidates,dig,outp,candp,migp);
    return 0;
}
