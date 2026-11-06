/* Declared contract projections. JSON emission uses the shared bf_jv/bf_jsonw owner. */
#include <math.h>
#include <sys/wait.h>
#include "contracts_templates.h"

static char cc_error[512];
static int cc_fail(const char *f, ...) { va_list ap; va_start(ap,f); vsnprintf(cc_error,sizeof cc_error,f,ap); va_end(ap); return 0; }
static const char *cc_s(const bf_jv *v,const char *k) { const char *s=bf_jv_s(bf_jv_get(v,k)); return s?s:""; }
static void cc_set(bf_jv *v,const char *k,const char *s) { bf_jv_set(v,k,bf_jv_str(s)); }
static char *cc_quote(const char *s) { bf_jv *v=bf_jv_str(s); char *r=bf_jv_dump(v,0,BF_JV_SORT_NONE,NULL); bf_jv_free(v); return r; }
static void cc_qset(bf_jv *v,const char *k,const char *s) { char *q=cc_quote(s); cc_set(v,k,q); free(q); }
static void cc_num(bf_jv *v,const char *k,size_t n) { char b[32]; snprintf(b,sizeof b,"%zu",n); cc_set(v,k,b); }
static Strs cc_words(char *s) { Strs out={0}; char *save=NULL; for(char *p=strtok_r(s," \t\r\v\f",&save);p;p=strtok_r(NULL," \t\r\v\f",&save))push(&out,p); return out; }
static char *cc_read(const char *path) { char *p=fmt("%s/%s",root,path),*s=slurp(p,NULL); if(!s)cc_fail("cannot read contract: %s",path); free(p); return s; }
static char *cc_render(const char *tmpl,const bf_jv *fields) {
    char *out=NULL; size_t n=0; FILE *f=open_memstream(&out,&n); if(!f)return NULL;
    for(const char *p=tmpl;*p;) { if(*p!='@'){fputc(*p++,f);continue;} const char *end=strchr(p+1,'@'); if(!end){fclose(f);free(out);return NULL;} char *key=strndup(p+1,(size_t)(end-p-1)); fputs(cc_s(fields,key),f); free(key);p=end+1; }
    if(fclose(f)){free(out);return NULL;}return out;
}
static void cc_json_file(bf_jv *files,const char *path,bf_jv *v,int order) { char *json=bf_jv_dump(v,2,order,NULL),*line=fmt("%s\n",json);cc_set(files,path,line);free(line);free(json);bf_jv_free(v); }
static int cc_header(bf_jv *files,const char *path,const char *tmpl,bf_jv *fields) { char *s=cc_render(tmpl,fields);bf_jv_free(fields);if(!s)return cc_fail("cannot render contract header");cc_set(files,path,s);free(s);return 1; }
static bf_jv *cc_doc(const char *schema_name,const char *path,const char *key,bf_jv *rows,const char *forbidden) { bf_jv *v=bf_jv_obj();cc_set(v,"schema_version",schema_name);cc_set(v,"source",path);bf_jv_set(v,"count",bf_jv_num((double)rows->len));bf_jv_set(v,key,rows);cc_set(v,"forbidden_inference",forbidden);return v; }
static int cc_lineages(bf_jv *files) {
    const char *path="architecture/apps/app-lineages.contract.yaff";char *text=cc_read(path);if(!text)return 0;bf_jv *rows=bf_jv_arr();
    for(char *save=NULL,*line=strtok_r(text,"\n",&save);line;line=strtok_r(NULL,"\n",&save)){
        if(strncmp(line,"  lineage ",10))continue;char *copy=strdup(line);Strs parts=cc_words(copy);
        if(parts.n<6){cc_fail("incomplete lineage: %s",line);free(copy);free(parts.v);goto fail;}
        bf_jv *row=bf_jv_obj();cc_set(row,"lineage",parts.v[1]);cc_set(row,"flow_stage",parts.v[3]);
        for(char *p=parts.v[4];*p;p++)if(*p=='-')*p=' ';cc_set(row,"representative_doctype",parts.v[4]);cc_set(row,"company_flow_after",strcmp(parts.v[5],"-")?parts.v[5]:"");
        for(int i=2;i<parts.n;i++){if(i>2&&i<6)continue;char *sep=strchr(parts.v[i],':');if(!sep||sep==parts.v[i]){cc_fail("invalid contract field: %s",parts.v[i]);bf_jv_free(row);free(copy);free(parts.v);goto fail;}*sep++=0;const char *key=!strcmp(parts.v[i],"dir")?"app_dir":parts.v[i];
            if((strcmp(key,"app_dir")&&strcmp(key,"dialect")&&strcmp(key,"owns")&&strcmp(key,"consumes")&&strcmp(key,"publishes"))||bf_jv_has(row,key)){cc_fail("unknown or repeated lineage field: %s",key);bf_jv_free(row);free(copy);free(parts.v);goto fail;}cc_set(row,key,strcmp(sep,"-")?sep:"");}
        bf_jv_push(rows,row);free(parts.v);free(copy);
    }
    if(!rows->len){cc_fail("app lineage contract declares no lineages");goto fail;}
    char *entries=NULL;size_t size=0;FILE *out=open_memstream(&entries,&size);if(!out){cc_fail("cannot render lineage rows");goto fail;}
    const char *keys[]={"lineage","app_dir","flow_stage","representative_doctype","company_flow_after","owns","consumes","publishes","dialect"};
    for(size_t i=0;i<rows->len;i++){if(i)fputs(", \\\n",out);fputs("    {",out);for(size_t k=0;k<9;k++){char *q=cc_quote(cc_s(rows->items[i],keys[k]));fprintf(out,"%s%s",k?", ":"",q);free(q);}fputc('}',out);}fclose(out);
    bf_jv *fields=bf_jv_obj();cc_set(fields,"entries",entries);cc_num(fields,"count",rows->len);free(entries);
    cc_json_file(files,"integrations/mcp/app_lineages_contract.json",cc_doc("bonfyre.app-lineages.contract.v1",path,"lineages",rows,"a declared lineage is not a running application, and a dialect records where a grammar was first extracted from rather than what serves it"),BF_JV_SORT_NONE);free(text);
    return cc_header(files,"engine/core/include/bf_app_lineages.h",cc_template_lineages,fields);
fail:free(text);bf_jv_free(rows);return 0;
}
static int cc_carriers(bf_jv *files,bf_jv **probe_rows) {
    const char *path="architecture/external/carriers.contract.yaff";char *text=cc_read(path);if(!text)return 0;bf_jv *rows=bf_jv_arr();int inside=0;
    for(char *save=NULL,*line=strtok_r(text,"\n",&save);line;line=strtok_r(NULL,"\n",&save)){
        char *clean=trim(line);if(!*clean||*clean=='#'){free(clean);continue;}if(*line!=' '){inside=!strncmp(line,"carrier_contract ",17);free(clean);continue;}if(!inside){free(clean);continue;}
        Strs p=cc_words(clean);if(p.n&&!strcmp(p.v[0],"carrier")){if(p.n!=3||p.v[2][0]!='/'){cc_fail("invalid carrier: %s",line);free(clean);free(p.v);goto fail;}bf_jv *row=bf_jv_obj();cc_set(row,"name",p.v[1]);cc_set(row,"path",p.v[2]);bf_jv_push(rows,row);}free(p.v);free(clean);
    }
    if(!rows->len){cc_fail("carrier contract declares no carriers");goto fail;}*probe_rows=bf_jv_clone(rows);
    cc_json_file(files,"integrations/mcp/carriers_contract.json",cc_doc("bonfyre.carriers.contract.v1",path,"carriers",rows,"a served carrier is not an authorized, priced, or willing one"),BF_JV_SORT_NONE);free(text);return 1;
fail:free(text);bf_jv_free(rows);return 0;
}
static char *cc_list(const bf_jv *values,int pairs) {
    char *s=NULL;size_t n=0;FILE *f=open_memstream(&s,&n);if(!f)return NULL;fputs("{ ",f);
    for(size_t i=0;i<values->len;i++){if(i)fputs(", ",f);if(pairs){char *q=cc_quote(values->keys[i]),*v=bf_jv_dump(values->items[i],0,BF_JV_SORT_NONE,NULL);fprintf(f,"{%s, %s}",q,v);free(q);free(v);}else{char *v=bf_jv_dump(values->items[i],0,BF_JV_SORT_NONE,NULL);fputs(v,f);free(v);}}
    fputs(" }",f);if(fclose(f)){free(s);return NULL;}return s;
}
static void cc_macro(bf_jv *fields,const char *key,const bf_jv *values,int pairs) { char *s=cc_list(values,pairs);cc_set(fields,key,s?s:"");free(s); }
static int cc_settlement(bf_jv *files) {
    char *text=cc_read("architecture/economic/settlement.contract.yaff");if(!text)return 0;bf_jv *fields=bf_jv_obj(),*carriers=bf_jv_arr();int inside=0;
    for(char *save=NULL,*line=strtok_r(text,"\n",&save);line;line=strtok_r(NULL,"\n",&save)){
        char *clean=trim(line);if(!*clean||*clean=='#'){free(clean);continue;}if(*line!=' '){inside=!strncmp(line,"settlement_contract ",20);free(clean);continue;}if(!inside){free(clean);continue;}
        char *space=strchr(clean,' ');if(!space){cc_fail("invalid settlement field: %s",clean);free(clean);goto fail;}*space++=0;char *value=trim(space);
        if(!strcmp(clean,"carrier"))bf_jv_push(carriers,bf_jv_str(value));else if(bf_jv_has(fields,clean)){cc_fail("repeated settlement field: %s",clean);free(clean);free(value);goto fail;}else cc_set(fields,clean,value);free(clean);free(value);
    }
    const char *required[]={"rail","quote_path","discovery_path"};for(size_t i=0;i<3;i++){if(!*cc_s(fields,required[i])){cc_fail("settlement contract is missing %s",required[i]);goto fail;}char *q=cc_quote(cc_s(fields,required[i]));cc_set(fields,required[i],q);free(q);}
    cc_macro(fields,"carriers",carriers,0);cc_num(fields,"count",carriers->len);bf_jv_free(carriers);free(text);return cc_header(files,"cmd/BonfyreWebAddress/src/bf_settlement.h",cc_template_settlement,fields);
fail:bf_jv_free(fields);bf_jv_free(carriers);free(text);return 0;
}
static bf_jv *cc_keys(bf_jv *obj) { bf_jv_sort_keys(obj,BF_JV_SORT_JS);bf_jv *a=bf_jv_arr();for(size_t i=0;i<obj->len;i++)bf_jv_push(a,bf_jv_str(obj->keys[i]));return a; }
static int cc_institutions(bf_jv *files) {
    const char *path="architecture/institutional/namespaces.yaff";char *text=cc_read(path);if(!text)return 0;
    bf_jv *roots=bf_jv_obj(),*managed=bf_jv_obj(),*parents=bf_jv_obj(),*inherited=bf_jv_obj(),*ranks=bf_jv_obj(),*leadership=bf_jv_obj(),*worlds=bf_jv_obj(),*personas=bf_jv_obj(),*slices=bf_jv_obj();char *current=strdup(""),*lattice=strdup("");
    for(char *save=NULL,*line=strtok_r(text,"\n",&save);line;line=strtok_r(NULL,"\n",&save)){
        if(!strncmp(line,"  role ",7)&&!*current){char *copy=strdup(line);Strs p=cc_words(copy);char *end=NULL;double rank=p.n>=3?strtod(p.v[2],&end):NAN;if(!isfinite(rank)||floor(rank)!=rank||!end||*end){cc_fail("invalid role rank: %s",line);free(p.v);free(copy);goto fail;}bf_jv_set(ranks,p.v[1],bf_jv_num(rank));free(p.v);free(copy);continue;}
        if(!strncmp(line,"persona_lattice ",16)){free(lattice);lattice=trim(line+16);if(!bf_jv_has(personas,lattice))bf_jv_set(personas,lattice,bf_jv_arr());free(current);current=strdup("");continue;}
        if(!strncmp(line,"  persona ",10)&&*lattice){char *copy=strdup(line);Strs p=cc_words(copy);if(p.n>=4){bf_jv *v=bf_jv_obj();cc_set(v,"persona",p.v[1]);cc_set(v,"rank",p.v[2]);cc_set(v,"capabilities",p.v[3]);bf_jv_push(bf_jv_get(personas,lattice),v);}free(copy);free(p.v);continue;}
        if(!strncmp(line,"institution_roles",17)){free(current);current=strdup("");free(lattice);lattice=strdup("");continue;}
        if(!strncmp(line,"namespace ",10)){free(current);current=trim(line+10);char *slash=strchr(current,'/');if(!slash)cc_set(roots,current,"");else if(!strncmp(slash,"/managed/",9)&&!strchr(slash+9,'/')){char *parent=strndup(current,(size_t)(slash-current));cc_set(managed,slash+9,parent);free(parent);}continue;}
        if(!*current)continue;char *clean=trim(line),*space=strchr(clean,' '),*value=space?trim(space+1):strdup("");char *slash=strchr(current,'/'),*last=strrchr(current,'/');
        if(!strncmp(line,"  inherited_role ",17)&&slash&&last!=slash&&strchr(slash+1,'/')==last&&bf_jv_has(managed,last+1))cc_set(inherited,last+1,value);
        else if(!slash){if(!strncmp(line,"  parent ",9))cc_set(parents,current,value);else if(!strncmp(line,"  delegated_parent ",19)&&!strcmp(value,"true"))cc_set(leadership,"estate",current);else if(!strncmp(line,"  leadership_role ",18))cc_set(leadership,"leadership_role",value);else if(!strncmp(line,"  leadership_permission ",24))cc_set(leadership,"leadership_permission",value);else if(!strncmp(line,"  leadership_object ",20))cc_set(leadership,"leadership_object",value);else if(!strncmp(line,"  world ",8))cc_set(worlds,current,value);}free(clean);free(value);
    }
    if(!roots->len||!managed->len){cc_fail("namespace contract declares no roots or no managed estates");goto fail;}
    for(size_t i=0;i<managed->len;i++)if(!bf_jv_has(parents,managed->keys[i]))cc_set(parents,managed->keys[i],bf_jv_s(managed->items[i]));
    if(leadership->len){const char *keys[]={"estate","leadership_role","leadership_permission","leadership_object"};for(size_t i=0;i<4;i++)if(!*cc_s(leadership,keys[i])){cc_fail("delegated parent declares an incomplete leadership tuple; missing %s",keys[i]);goto fail;}}
    for(size_t i=0;i<roots->len;i++){const char *r=roots->keys[i];if(!bf_jv_has(worlds,r))cc_set(worlds,r,"LIVE");if(strcmp(r,"bonfyre"))cc_set(slices,r,"");}for(size_t i=0;i<managed->len;i++)cc_set(slices,managed->keys[i],"");
    bf_jv *rs=cc_keys(roots),*ms=cc_keys(managed),*ss=cc_keys(slices);bf_jv_sort_keys(parents,BF_JV_SORT_JS);bf_jv_sort_keys(inherited,BF_JV_SORT_JS);bf_jv_sort_keys(worlds,BF_JV_SORT_JS);
    /* Stable insertion sort preserves declaration order for tied role ranks. */
    Strs rank_names={0};for(size_t i=0;i<ranks->len;i++)push(&rank_names,ranks->keys[i]);
    for(int i=1;i<rank_names.n;i++)for(int j=i;j&&bf_jv_n(bf_jv_get(ranks,rank_names.v[j-1]),0)>bf_jv_n(bf_jv_get(ranks,rank_names.v[j]),0);j--){char *k=rank_names.v[j];rank_names.v[j]=rank_names.v[j-1];rank_names.v[j-1]=k;}
    bf_jv *ordered_ranks=bf_jv_obj();for(int i=0;i<rank_names.n;i++)bf_jv_set(ordered_ranks,rank_names.v[i],bf_jv_clone(bf_jv_get(ranks,rank_names.v[i])));free(rank_names.v);bf_jv_free(ranks);ranks=ordered_ranks;
    bf_jv *fields=bf_jv_obj();cc_macro(fields,"BF_INSTITUTION_ROOTS",rs,0);cc_num(fields,"BF_INSTITUTION_ROOT_N",rs->len);cc_macro(fields,"BF_MANAGED_ESTATES",ms,0);cc_num(fields,"BF_MANAGED_ESTATE_N",ms->len);cc_macro(fields,"BF_SLICE_ESTATES",ss,0);cc_num(fields,"BF_SLICE_ESTATE_N",ss->len);cc_macro(fields,"BF_INSTITUTION_PARENTS",parents,1);cc_num(fields,"BF_INSTITUTION_PARENT_N",parents->len);cc_macro(fields,"BF_INHERITED_ROLES",inherited,1);cc_num(fields,"BF_INHERITED_ROLE_N",inherited->len);cc_macro(fields,"BF_ROLE_RANKS",ranks,1);cc_num(fields,"BF_ROLE_RANK_N",ranks->len);cc_macro(fields,"BF_INSTITUTION_WORLDS",worlds,1);cc_num(fields,"BF_INSTITUTION_WORLD_N",worlds->len);
    const char *macros[]={"BF_DELEGATED_PARENT","BF_DELEGATED_LEAD_ROLE","BF_DELEGATED_LEAD_PERM","BF_DELEGATED_LEAD_OBJECT"},*keys[]={"estate","leadership_role","leadership_permission","leadership_object"};for(size_t i=0;i<4;i++)cc_qset(fields,macros[i],cc_s(leadership,keys[i]));char *scope=fmt("institution:%s",cc_s(leadership,"estate"));cc_qset(fields,"BF_DELEGATED_LEAD_SCOPE",scope);free(scope);
    bf_jv *json=bf_jv_obj();cc_set(json,"schema_version","bonfyre.institution.contract.v1");cc_set(json,"source",path);bf_jv_set(json,"roots",rs);bf_jv_set(json,"managed_estates",ms);bf_jv_set(json,"slice_estates",ss);bf_jv_set(json,"parents",bf_jv_clone(parents));bf_jv_set(json,"inherited_roles",bf_jv_clone(inherited));bf_jv_set(json,"role_ranks",bf_jv_clone(ranks));bf_jv_set(json,"delegated_leadership",bf_jv_clone(leadership));bf_jv_set(json,"worlds",bf_jv_clone(worlds));bf_jv_set(json,"persona_lattice",bf_jv_clone(personas));cc_set(json,"note","the estates an authenticated account may hold a slice in; membership is declared here, admission is decided by the grant");cc_set(json,"forbidden_inference","a declared estate is not a granted one, and this list is not an authority model: it says which estates exist, never who may enter one");
    cc_json_file(files,"integrations/mcp/institution_contract.json",json,BF_JV_SORT_JS);if(!cc_header(files,"engine/core/include/bf_institutions.h",cc_template_institutions,fields))goto fail;
    bf_jv_free(roots);bf_jv_free(managed);bf_jv_free(parents);bf_jv_free(inherited);bf_jv_free(ranks);bf_jv_free(leadership);bf_jv_free(worlds);bf_jv_free(personas);bf_jv_free(slices);free(current);free(lattice);free(text);return 1;
fail:bf_jv_free(roots);bf_jv_free(managed);bf_jv_free(parents);bf_jv_free(inherited);bf_jv_free(ranks);bf_jv_free(leadership);bf_jv_free(worlds);bf_jv_free(personas);bf_jv_free(slices);free(current);free(lattice);free(text);return 0;
}
static int cc_probe(const char *origin,const bf_jv *rows) {
    const char *scheme=strstr(origin,"://");if(!scheme||(!strncmp(origin,"http://",7)?0:strncmp(origin,"https://",8)))return cc_fail("carrier origin requires http:// or https://");const char *end=strchr(scheme+3,'/');size_t base=end?(size_t)(end-origin):strlen(origin);int failed=0;
    for(size_t i=0;i<rows->len;i++){const char *path=cc_s(rows->items[i],"path"),*name=cc_s(rows->items[i],"name");char *url=!strncmp(path,"//",2)?fmt("%.*s:%s",(int)(scheme-origin),origin,path):fmt("%.*s%s",(int)base,origin,path);int fds[2];if(pipe(fds)){free(url);return cc_fail("carrier probe pipe failed");}pid_t pid=fork();if(pid==0){close(fds[0]);dup2(fds[1],STDOUT_FILENO);close(fds[1]);FILE *quiet=freopen("/dev/null","w",stderr);(void)quiet;execl("/usr/bin/curl","curl","--silent","--location","--max-time","15","--output","/dev/null","--write-out","%{http_code}","--header","Accept: application/json","--header","Host: aurekai.life","--url",url,(char *)NULL);_exit(127);}close(fds[1]);char status[16]={0};ssize_t got=read(fds[0],status,sizeof status-1);close(fds[0]);int code=0;if(pid<0||waitpid(pid,&code,0)<0||!WIFEXITED(code)||WEXITSTATUS(code)||got<=0){fprintf(stderr,"  %-12s unreachable: native HTTP probe failed\n",name);failed++;}else{int n=atoi(status),ok=n==200||n==402;printf("  %-12s %d %s\n",name,n,ok?"":"<-- declared but not serving");if(!ok)failed++;}free(url);}
    return failed?cc_fail("%d declared carrier(s) not serving",failed):1;
}
static int cmd_contracts(int argc,char **argv) {
    if(!argc|| (strcmp(argv[0],"app-lineages")&&strcmp(argv[0],"carriers")&&strcmp(argv[0],"settlement")&&strcmp(argv[0],"institutions"))){fprintf(stderr,"usage: bonfyre-atlas compile-contracts <app-lineages|carriers|settlement|institutions> [--check|--check-generated] [--origin=URL]\n");return 2;}
    int check=0,probe=0;const char *origin="http://127.0.0.1:8787";for(int i=1;i<argc;i++){if(!strcmp(argv[i],"--check")){check=1;probe=1;}else if(!strcmp(argv[i],"--check-generated"))check=1;else if(!strncmp(argv[i],"--origin=",9))origin=argv[i]+9;else{fprintf(stderr,"unknown contract option: %s\n",argv[i]);return 2;}}
    bf_jv *files=bf_jv_obj(),*rows=NULL;cc_error[0]=0;int ok=!strcmp(argv[0],"app-lineages")?cc_lineages(files):!strcmp(argv[0],"carriers")?cc_carriers(files,&rows):!strcmp(argv[0],"settlement")?cc_settlement(files):cc_institutions(files);
    for(size_t i=0;ok&&i<files->len;i++){const char *rel=files->keys[i],*want=bf_jv_s(files->items[i]);char *target=fmt("%s/%s",root,rel);if(check){char *have=slurp(target,NULL);if(!have||strcmp(have,want))ok=cc_fail("%s disagrees with %s contract; re-run cmd/BonfyreAtlas/bonfyre-atlas compile-contracts %s",rel,argv[0],argv[0]);free(have);}else{char *parent=strdup(target),*slash=strrchr(parent,'/');if(slash)*slash=0;mkdirs(parent);free(parent);char *tmp=fmt("%s.XXXXXX",target);int fd=mkstemp(tmp);if(fd<0)ok=cc_fail("cannot write generated contract: %s",rel);else{size_t n=strlen(want);FILE *f=fdopen(fd,"w");if(!f){close(fd);ok=0;}else{ok=fwrite(want,1,n,f)==n;if(fclose(f))ok=0;}if(ok)ok=chmod(tmp,0644)==0&&rename(tmp,target)==0;if(!ok)cc_fail("cannot publish generated contract: %s",rel);}if(!ok)unlink(tmp);else printf("wrote %s\n",rel);free(tmp);}free(target);}
    if(ok&&probe&&!strcmp(argv[0],"carriers"))ok=cc_probe(origin,rows);if(ok&&check)printf("%s contract agrees\n",argv[0]);if(!ok)fprintf(stderr,"%s\n",*cc_error?cc_error:"contract compilation failed");bf_jv_free(files);bf_jv_free(rows);return ok?0:1;
}
