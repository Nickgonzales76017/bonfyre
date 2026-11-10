/* Thirty full-experience maturation contracts, compiled directly from YAFF.
 * This is the existing ExperienceStage owner's admission/preflight adapter.
 * It reports source/witness presence, never execution or publication by implication.
 * No JSON runtime state, duplicate application store, or synthetic "ready" claims.
 */
#define _POSIX_C_SOURCE 200809L
#include "portfolio.h"
#include <ctype.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

enum { PORTFOLIO_COUNT=30, STAGES_MIN=5, STAGES_MAX=12 };
typedef struct {
    int id, nstages; unsigned fields; uint32_t depends;
    char slug[80], category[40], owner[240], entry[340],
         raw_deps[100], stages[300], release_gate[160], witness[250];
} Program;
typedef struct { Program p[PORTFOLIO_COUNT]; int count; } Portfolio;
static int error_at(int line,const char *message) {
    fprintf(stderr,"experience portfolio refused at line %d: %s\n",line,message);return 2;
}
static int copy_field(char *target,size_t cap,const char *value) {
    size_t len=strlen(value);
    if(!len||len>=cap)return 0;
    memcpy(target,value,len+1);return 1;
}
static int ident(const char *value) {
    if(!value||!*value)return 0;
    for(const unsigned char *p=(const unsigned char*)value;*p;p++)
        if(!isalnum(*p)&&*p!='-'&&*p!='_'&&*p!='.')return 0;
    return 1;
}
static int source_path(const char *v) {
    if(!v||!*v||*v=='/'||strstr(v,"..")||strchr(v,'\\')||strchr(v,':'))return 0;
    for(const unsigned char *p=(const unsigned char*)v;*p;p++)
        if(!isalnum(*p)&&*p!='-'&&*p!='_'&&*p!='.'&&*p!='/')return 0;
    return 1;
}
static int entry_url(const char *v,const char *type){
    if(!strcmp(v,"-"))return !strcmp(type,"new-program");
    if(!strncmp(v,"https://aurekai-work-samples.pages.dev/",39) &&
       (strcmp(type,"new-program")==0))return 0;
    const char *prefixes[]={
        "https://aurekai-work-samples.pages.dev/",
        "https://brief-studio-upgrade.aurekai-work-samples.pages.dev/",
        "https://itad-desk-upgrade.aurekai-work-samples.pages.dev/",
        "https://docs.aurekai.work/",
        "https://nickgonzales76017.github.io/pages-"
    };
    int allowed=0;
    for(size_t i=0;i<sizeof(prefixes)/sizeof(*prefixes);i++)
        if(!strncmp(v,prefixes[i],strlen(prefixes[i])))allowed=1;
    if(!allowed||strchr(v,'?')||strchr(v,'#'))return 0;
    for(const unsigned char *p=(const unsigned char*)v;*p;p++)
        if(!isalnum(*p)&&*p!='-'&&*p!='_'&&*p!='.'&&*p!='/'&&*p!=':')return 0;
    return 1;
}
static int stages_count(const char *s) {
    int n=1;char copy[300];
    if(!copy_field(copy,sizeof(copy),s))return 0;
    char *part=copy;
    while(part && *part) {
        char *next=strchr(part,'|');
        if(next)*next='\0';
        if(!ident(part))return 0;
        if(next){n++;part=next+1;}else break;
    }
    return n>=STAGES_MIN&&n<=STAGES_MAX?n:0;
}
static int parse_deps(Program *p) {
    if(!strcmp(p->raw_deps,"-")){p->depends=0;return 1;}
    char buf[100];if(!copy_field(buf,sizeof(buf),p->raw_deps))return 0;
    char *token=buf;
    while(token){
        char *end=strchr(token,',');
        if(end)*end='\0';
        int id=0;
        if(!*token)return 0;
        for(const char *q=token;*q;q++){
            if(!isdigit((unsigned char)*q))return 0;
            id=id*10+*q-'0';
            if(id>PORTFOLIO_COUNT)return 0;
        }
        if(id<1||id==p->id||(p->depends&(1u<<(id-1))))return 0;
        p->depends|=1u<<(id-1);
        token=end?end+1:NULL;
    }
    return 1;
}
static int parse(const char *path,Portfolio *out) {
    FILE *f=fopen(path,"rb");char *line=NULL;size_t cap=0;ssize_t n;int lineno=0;
    if(!f)return error_at(0,"source YAFF is unavailable");
    memset(out,0,sizeof(*out));
    Program *current=NULL;
    while((n=getline(&line,&cap,f))>=0){
        lineno++;if(n>4096){free(line);fclose(f);return error_at(lineno,"unbounded declaration");}
        while(n>0&&(line[n-1]=='\n'||line[n-1]=='\r'))line[--n]='\0';
        if(!n||line[0]=='#')continue;
        if(!strncmp(line,"maturity ",9)){
            if(out->count>=PORTFOLIO_COUNT){free(line);fclose(f);return error_at(lineno,"more than 30 programs");}
            unsigned id=0;char slug[80]={0},extra;
            if(sscanf(line+9,"%u %79s %c",&id,slug,&extra)!=2||id!=(unsigned)out->count+1||!ident(slug)){
                free(line);fclose(f);return error_at(lineno,"maturity ID/order or slug invalid");
            }
            current=&out->p[out->count++];
            current->id=(int)id;copy_field(current->slug,sizeof(current->slug),slug);continue;
        }
        if(line[0]==' '&&line[1]==' '&&current){
            char *key=line+2,*value=strchr(key,' ');
            if(!value||value==key){free(line);fclose(f);return error_at(lineno,"field must have a value");}
            *value++='\0';if(!*value){free(line);fclose(f);return error_at(lineno,"blank field");}
            char *dest=NULL;size_t size=0;unsigned bit=0;
            if(!strcmp(key,"class")){dest=current->category;size=sizeof(current->category);bit=1u;}
            else if(!strcmp(key,"owner")){dest=current->owner;size=sizeof(current->owner);bit=2u;}
            else if(!strcmp(key,"entry")){dest=current->entry;size=sizeof(current->entry);bit=4u;}
            else if(!strcmp(key,"depends")){dest=current->raw_deps;size=sizeof(current->raw_deps);bit=8u;}
            else if(!strcmp(key,"stages")){dest=current->stages;size=sizeof(current->stages);bit=16u;}
            else if(!strcmp(key,"release_gate")){dest=current->release_gate;size=sizeof(current->release_gate);bit=32u;}
            else if(!strcmp(key,"witness")){dest=current->witness;size=sizeof(current->witness);bit=64u;}
            else {free(line);fclose(f);return error_at(lineno,"unknown YAFF field");}
            if((current->fields&bit)||!copy_field(dest,size,value)){
                free(line);fclose(f);return error_at(lineno,"duplicate or oversized YAFF field");
            }
            current->fields|=bit;continue;
        }
        if(!strncmp(line,"pack ",5)||!strncmp(line,"generation ",11)||
           !strncmp(line,"authority ",10)||!strncmp(line,"basis ",6)||
           !strncmp(line,"invariant ",10))continue;
        free(line);fclose(f);return error_at(lineno,"unrecognized top-level declaration");
    }
    free(line);fclose(f);
    if(out->count!=PORTFOLIO_COUNT)return error_at(lineno,"exactly thirty maturity programs required");
    int classes[5]={0};
    for(int i=0;i<out->count;i++) {
        Program *p=&out->p[i];if(p->fields!=127u||!source_path(p->owner)||
           (strcmp(p->witness,"-")&&!source_path(p->witness))||
           !ident(p->release_gate)||!(p->nstages=stages_count(p->stages))||
           !parse_deps(p))return error_at(i+1,"invalid typed program fields or dependency");
        const char *names[]={"existing-tool","case-study","local-app-guide","branded-sites-guide","github-foundation","new-program"};
        int recognized=0;for(size_t j=0;j<sizeof(names)/sizeof(*names);j++)
            if(!strcmp(p->category,names[j]))recognized=1;
        if(!recognized||!entry_url(p->entry,p->category))
            return error_at(i+1,"unrecognized class or unsuitable public route");
        classes[!strcmp(p->category,"new-program")?2:!strcmp(p->category,"github-foundation")?1:0]++;
        for(int j=0;j<i;j++)if(!strcmp(p->slug,out->p[j].slug))
            return error_at(i+1,"duplicate maturity identity");
    }
    if(classes[0]!=8||classes[1]!=8||classes[2]!=14)
        return error_at(lineno,"the required 8/8/14 distinction drifted");
    uint32_t emitted=0;
    for(int k=0;k<PORTFOLIO_COUNT;k++){
        int advanced=0;
        for(int i=0;i<PORTFOLIO_COUNT;i++){
            uint32_t bit=1u<<i;
            if(!(emitted&bit)&&(out->p[i].depends&emitted)==out->p[i].depends){
                emitted|=bit;advanced=1;break;
            }
        }
        if(!advanced)return error_at(lineno,"dependency cycle detected");
    }
    return 0;
}
static void html(FILE *out,const char *s){
    for(const unsigned char *p=(const unsigned char*)s;*p;p++){
        if(*p=='&')fputs("&amp;",out);
        else if(*p=='<')fputs("&lt;",out);
        else if(*p=='>')fputs("&gt;",out);
        else if(*p=='"')fputs("&quot;",out);
        else fputc(*p,out);
    }
}
static int has_source(const char *root,const char *relative){
    if(!source_path(relative))return 0;
    char path[4096],real[4096],base[4096];
    if(snprintf(path,sizeof(path),"%s/%s",root,relative)>=(int)sizeof(path)||
       !realpath(root,base)||!realpath(path,real))return 0;
    size_t n=strlen(base);if(strncmp(base,real,n)||real[n]!='/')return 0;
    struct stat st;
    return stat(real,&st)==0&&(S_ISREG(st.st_mode)||S_ISDIR(st.st_mode));
}
static const char *surface_kind(const Program *p) {
    if(!strcmp(p->category,"existing-tool"))return "working-sample";
    if(!strcmp(p->category,"case-study"))return "case-study";
    if(!strcmp(p->category,"local-app-guide")||!strcmp(p->category,"branded-sites-guide"))return "guide";
    if(!strcmp(p->category,"github-foundation"))return "legacy-foundation";
    if(strcmp(p->entry,"-"))return "published-preview";
    return "not-published";
}
static int audit(const Portfolio *items,const char *root){
    int missing=0;
    for(int i=0;i<items->count;i++){
        const Program *p=&items->p[i];
        int present=has_source(root,p->owner);
        int witnessed=strcmp(p->witness,"-")&&has_source(root,p->witness);
        printf("%02d\t%s\t%s\towner=%s\twitness_source=%s\tpublic_kind=%s\tgate=%s\n",
            p->id,p->slug,p->category,present?"present":"missing",
            witnessed?"present_unexecuted":"absent",surface_kind(p),p->release_gate);
        missing+=!present;
    }
    printf("PORTFOLIO\tprograms=%d\tclasses=8+8+14\tmissing_owner=%d\tapproved_publications=0\tnew_demo_claims=0\n",
        items->count,missing);
    return missing?3:0;
}
static void inspect(const Program *p,const char *root){
    printf("PROGRAM\t%02d\t%s\tclass=%s\n",p->id,p->slug,p->category);
    printf("OWNER\t%s\t%s\n",p->owner,has_source(root,p->owner)?"source_present":"source_missing");
    printf("WITNESS\t%s\t%s\n",p->witness,
        strcmp(p->witness,"-")&&has_source(root,p->witness)?"source_present_not_executed":"unavailable");
    printf("ENTRY\t%s\t%s\n",p->entry,surface_kind(p));
    printf("RELEASE_GATE\t%s\tstate=unwitnessed\n",p->release_gate);
    printf("DEPENDENCIES\t%s\n",p->raw_deps);
    char steps[300];copy_field(steps,sizeof(steps),p->stages);
    char *part=steps;int n=0;
    while(part){char *sep=strchr(part,'|');if(sep)*sep='\0';
        printf("STAGE\t%d\t%s\n",++n,part);part=sep?sep+1:NULL;}
    puts("CANNOT_INFER\tsource or witnessed test path exists -> completed user workflow, admitted evidence, delegated authority, provider availability or live public release");
}
static int order(const Portfolio *list){
    uint32_t emitted=0;
    for(int step=0;step<PORTFOLIO_COUNT;step++){
        int best=-1;
        for(int i=0;i<list->count;i++){
            uint32_t bit=1u<<i;
            if(!(emitted&bit)&&(list->p[i].depends&emitted)==list->p[i].depends){
                best=i;break;
            }
        }
        if(best<0)return 3;
        emitted|=1u<<best;
        printf("%02d\tprogram=%02d\t%s\tgate=%s\n",step+1,
            list->p[best].id,list->p[best].slug,list->p[best].release_gate);
    }
    puts("ORDER\tdependency_only_not_authorization_or_work_claim");
    return 0;
}
static int cards(const Portfolio *list,const char *root){
    int count=0;puts("<section class=\"bf-experience-portfolio\" aria-label=\"Existing experience routes\"><ul>");
    for(int i=0;i<list->count;i++){
        const Program *p=&list->p[i];
        if(!strcmp(p->entry,"-"))continue;
        const char *kind=surface_kind(p);
        fputs("<li data-state=\"",stdout);html(stdout,kind);fputs("\"><a href=\"",stdout);
        html(stdout,p->entry);fputs("\"><strong>",stdout);html(stdout,p->slug);fputs("</strong></a><span>",stdout);
        html(stdout,kind);fputs("</span><small>",stdout);
        html(stdout,has_source(root,p->owner)?"Source available; release must be verified":"Source owner missing");
        fputs("</small></li>\n",stdout);count++;
    }
    puts("</ul></section>");
    fprintf(stderr,"portfolio cards: %d existing routes only; no unbuilt route advertised\n",count);
    return 0; /* The authored route set grows independently of program origin. */
}
/* Queue submission is a separate human effect. This is a TSV plan for Queue's
 * existing enqueue-batch format; only zero-dependency roots qualify, because
 * enqueue-batch does not encode prerequisite edges. No operator is resumed. */
static int queue_plan(const Portfolio *p,const char *root) {
    int emitted=0;
    for(int i=0;i<p->count;i++) {
        const Program *item=&p->p[i];
        if(item->depends)continue;
        if(!has_source(root,item->owner))
            return error_at(item->id,"root owner source unavailable; no queue artifact emitted");
    }
    for(int i=0;i<p->count;i++) {
        const Program *item=&p->p[i];
        if(item->depends)continue;
        printf("maturity-%02d-%s\treview-experience\tbonfyre://experience/maturity/%02d"
               "\texperience-review\t100\t0\t1000\n",item->id,item->slug,item->id);
        emitted++;
    }
    fprintf(stderr,"experience portfolio: %d dependency roots staged as review-only "
            "work candidates; other %d maturity effects remain held by prerequisite gates\n",
            emitted,p->count-emitted);
    return emitted>0?0:3;
}
int bf_portfolio_main(int argc,char **argv){
    const char *verb=argc>2?argv[2]:"audit",*contract="experience-maturities.yaff",*root="../..";
    for(int i=3;i+1<argc;i++){
        if(!strcmp(argv[i],"--contracts"))contract=argv[++i];
        else if(!strcmp(argv[i],"--root"))root=argv[++i];
        else if(argv[i][0]=='-'&&argv[i][1]=='-'){fprintf(stderr,"portfolio option refused: %s\n",argv[i]);return 2;}
    }
    Portfolio *p=calloc(1,sizeof(*p));if(!p)return 2;
    int rc=parse(contract,p);
    if(!rc){
        if(!strcmp(verb,"audit"))rc=audit(p,root);
        else if(!strcmp(verb,"order"))rc=order(p);
        else if(!strcmp(verb,"cards"))rc=cards(p,root);
        else if(!strcmp(verb,"queue-plan"))rc=queue_plan(p,root);
        else if(!strcmp(verb,"show")) {
            int id=argc>3?atoi(argv[3]):0;
            if(id<1||id>PORTFOLIO_COUNT)rc=error_at(0,"show needs program number 1..30");
            else inspect(&p->p[id-1],root);
        } else rc=error_at(0,"portfolio audit|order|cards|show ID");
    }
    free(p);return rc;
}
