#ifndef BONFYRE_ACCOUNT_WORLD_LITERATURE_WEB_H
#define BONFYRE_ACCOUNT_WORLD_LITERATURE_WEB_H
#include "../../BonfyreCMS/src/account_world_literature.h"
#include "bf_command_lookup.h"

/* Writing inside account spaces (account-world-literature-1).
 *
 * /workspace/world-literature           private API: works, passages, choices, glosses; readers' places
 * studio section on /workspace/worlds   the owner's writing desk for the same, with earlier versions compared
 * /workspace/worlds/reading             a signed-in reader keeps and resumes their place
 * /worlds/X                             the space's bibliography; "works/<key>" items show the text itself
 * /worlds/X/read/W[/P]                  the reader: a poem line for line, a serial's contents and
 *                                       chapters on their release day, an interactive story by its declared
 *                                       choices, glosses as numbered notes
 *
 * Every word of a passage is escaped and nothing in it runs; the only
 * formatting read from the text is _emphasis_ inside one line. A recitation
 * plays only when its own source states open terms (world_web_open_terms) at
 * the time of the request. Included from account_world_web.h after audio. */

/* defined further down in account_world_web.h */
static void world_web_public_head(int code,const char *title,const char *site_name,const char *lede);
static void world_web_hidden_base(const char *site,sqlite3_int64 world,const char *space,const char *op,long long revision);

/* ------------------------------------------------------------- loading */
typedef struct{char work[65],key[25],kind[16],title[257],author[161],translator[161],language[17],original[65],collection[81],
               rights[16],licence[129],provenance[513],provenance_label[161],recitation[100];int position;} WlWork;
typedef struct{char passage[65],title[257],release_on[11];int number,start;char *body;} WlPassage;
typedef struct{char passage[65],label[201],target[65];int number;} WlChoice;
typedef struct{long long id;char passage[65],kind[10],body[1001];int unit;} WlGloss;
typedef struct{WlWork w;WlPassage *p;int np;WlChoice *c;int nc;WlGloss *g;int ng;} WlText;

static const char *wl_col(sqlite3_stmt *s,int i){const char *v=(const char*)sqlite3_column_text(s,i);return v?v:"";}
static void wl_work_row(sqlite3_stmt *s,WlWork *w){
    char *dst[]={w->work,w->key,w->kind,w->title,w->author,w->translator,w->language,w->original,w->collection,w->rights,w->licence,w->provenance,w->provenance_label,w->recitation};
    size_t cap[]={sizeof(w->work),sizeof(w->key),sizeof(w->kind),sizeof(w->title),sizeof(w->author),sizeof(w->translator),sizeof(w->language),sizeof(w->original),
                  sizeof(w->collection),sizeof(w->rights),sizeof(w->licence),sizeof(w->provenance),sizeof(w->provenance_label),sizeof(w->recitation)};
    for(int i=0;i<14;i++)snprintf(dst[i],cap[i],"%s",wl_col(s,i));w->position=sqlite3_column_int(s,14);
}
/* subject NULL = released public copy */
static int wl_works(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 world,const char *space,WlWork **out){
    sqlite3_stmt *s=NULL;int n=0;*out=calloc(AWL_WORK_LIMIT,sizeof(WlWork));if(!*out)return 0;
    if(awl_rows(db,subject,ns,world,space,AWL_WORKS,NULL,&s)==1)while(n<AWL_WORK_LIMIT&&sqlite3_step(s)==SQLITE_ROW)wl_work_row(s,&(*out)[n++]);
    sqlite3_finalize(s);return n;
}
static void wl_free(WlText *t){for(int i=0;i<t->np;i++)free(t->p[i].body);free(t->p);free(t->c);free(t->g);memset(t,0,sizeof(*t));}
static int wl_load(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 world,const char *space,const char *work,WlText *t){
    memset(t,0,sizeof(*t));sqlite3_stmt *s=NULL;int found=0;
    if(awl_rows(db,subject,ns,world,space,AWL_WORKS,work,&s)==1&&sqlite3_step(s)==SQLITE_ROW){wl_work_row(s,&t->w);found=1;}
    sqlite3_finalize(s);s=NULL;if(!found)return 0;
    t->p=calloc(AWL_PASSAGE_LIMIT,sizeof(WlPassage));t->c=calloc(AWL_PASSAGE_LIMIT*AWL_CHOICE_LIMIT,sizeof(WlChoice));t->g=calloc(AWL_GLOSS_LIMIT,sizeof(WlGloss));
    if(!t->p||!t->c||!t->g){wl_free(t);return 0;}
    if(awl_rows(db,subject,ns,world,space,AWL_PASSAGES,work,&s)==1)while(t->np<AWL_PASSAGE_LIMIT&&sqlite3_step(s)==SQLITE_ROW){WlPassage *p=&t->p[t->np];
        snprintf(p->passage,sizeof(p->passage),"%s",wl_col(s,1));p->number=sqlite3_column_int(s,2);snprintf(p->title,sizeof(p->title),"%s",wl_col(s,3));
        p->body=strdup(wl_col(s,4));snprintf(p->release_on,sizeof(p->release_on),"%s",wl_col(s,5));p->start=sqlite3_column_int(s,6);if(!p->body)break;t->np++;}
    sqlite3_finalize(s);s=NULL;
    if(awl_rows(db,subject,ns,world,space,AWL_CHOICES,work,&s)==1)while(t->nc<AWL_PASSAGE_LIMIT*AWL_CHOICE_LIMIT&&sqlite3_step(s)==SQLITE_ROW){WlChoice *c=&t->c[t->nc++];
        snprintf(c->passage,sizeof(c->passage),"%s",wl_col(s,1));c->number=sqlite3_column_int(s,2);snprintf(c->label,sizeof(c->label),"%s",wl_col(s,3));snprintf(c->target,sizeof(c->target),"%s",wl_col(s,4));}
    sqlite3_finalize(s);s=NULL;
    if(awl_rows(db,subject,ns,world,space,AWL_GLOSSES,work,&s)==1)while(t->ng<AWL_GLOSS_LIMIT&&sqlite3_step(s)==SQLITE_ROW){WlGloss *g=&t->g[t->ng++];
        g->id=sqlite3_column_int64(s,0);snprintf(g->passage,sizeof(g->passage),"%s",wl_col(s,2));g->unit=sqlite3_column_int(s,3);
        snprintf(g->kind,sizeof(g->kind),"%s",wl_col(s,4));snprintf(g->body,sizeof(g->body),"%s",wl_col(s,5));}
    sqlite3_finalize(s);return 1;
}
static int wl_find(const WlText *t,const char *passage){for(int i=0;i<t->np;i++)if(!strcmp(t->p[i].passage,passage))return i;return -1;}
static void wl_today(char out[11]){time_t now=time(NULL);struct tm g;gmtime_r(&now,&g);strftime(out,11,"%Y-%m-%d",&g);}
static int wl_open(const WlPassage *p,const char *today){return !p->release_on[0]||strcmp(p->release_on,today)<=0;}
static int wl_first(const WlText *t){int best=-1;for(int i=0;i<t->np;i++)if(t->p[i].start){best=i;break;}return best>=0?best:t->np?0:-1;}
/* Passages an interactive reader can reach from the first one along declared
 * choices; parent[] gives one shortest way back. */
static void wl_reach(const WlText *t,char *reach,int *parent){
    memset(reach,0,(size_t)t->np);for(int i=0;i<t->np;i++)parent[i]=-1;
    int *queue=malloc(sizeof(int)*(size_t)(t->np+1)),head=0,tail=0,first=wl_first(t);if(!queue||first<0){free(queue);return;}
    reach[first]=1;queue[tail++]=first;
    while(head<tail){int at=queue[head++];
        for(int c=0;c<t->nc;c++)if(!strcmp(t->c[c].passage,t->p[at].passage)){int to=wl_find(t,t->c[c].target);if(to>=0&&!reach[to]){reach[to]=1;parent[to]=at;queue[tail++]=to;}}}
    free(queue);
}
static const char *wl_kind_word(const char *k){
    return !strcmp(k,"poem")?"Poem":!strcmp(k,"story")?"Story":!strcmp(k,"essay")?"Essay":!strcmp(k,"memoir")?"Memoir":!strcmp(k,"serial")?"Serial":
           !strcmp(k,"interactive")?"A story you steer":k;
}
static const char *wl_language_word(const char *l){
    static const struct{const char *code,*name;}names[]={{"en","English"},{"es","Spanish"},{"fr","French"},{"de","German"},{"it","Italian"},{"pt","Portuguese"},
        {"ru","Russian"},{"ja","Japanese"},{"zh","Chinese"},{"ar","Arabic"},{"la","Latin"},{"el","Greek"},{"grc","Ancient Greek"},{"pl","Polish"},{"nl","Dutch"},{"yi","Yiddish"}};
    char base[17];size_t n=0;for(;l&&l[n]&&l[n]!='-'&&n+1<sizeof(base);n++)base[n]=(char)tolower((unsigned char)l[n]);base[n]=0;
    for(size_t i=0;i<sizeof(names)/sizeof(names[0]);i++)if(!strcmp(base,names[i].code))return names[i].name;
    return l?l:"";
}

/* ------------------------------------------------------------- text */
static void wl_esc(const char *s,size_t n){
    for(size_t i=0;i<n;i++){unsigned char c=(unsigned char)s[i];
        if(c=='&')fputs("&amp;",stdout);else if(c=='<')fputs("&lt;",stdout);else if(c=='>')fputs("&gt;",stdout);else if(c=='"')fputs("&quot;",stdout);
        else if(c=='\'')fputs("&#39;",stdout);else fputc(c,stdout);}
}
/* One line: escaped, with _emphasis_ kept when it opens and closes inside the line. */
static void wl_inline(const char *s,size_t n){
    size_t i=0,from=0;
    while(i<n){
        if(s[i]=='_'&&i+2<n&&s[i+1]!=' '&&s[i+1]!='_'&&(i==0||!isalnum((unsigned char)s[i-1]))){
            size_t j=i+2;while(j<n&&!(s[j]=='_'&&s[j-1]!=' '&&(j+1==n||!isalnum((unsigned char)s[j+1]))))j++;
            if(j<n){wl_esc(s+from,i-from);ui_open(stdout,"em",NULL);wl_esc(s+i+1,j-i-1);ui_close(stdout,"em");i=j+1;from=i;continue;}
        }
        i++;
    }
    wl_esc(s+from,n-from);
}
static int wl_is_poem(const char *kind){return !strcmp(kind,"poem");}
/* Markers for the glosses on one unit; *seq numbers them in reading order. */
static void wl_markers(const WlText *t,const char *passage,int unit,int *seq){
    for(int g=0;g<t->ng;g++){const WlGloss *x=&t->g[g];if(x->unit!=unit||strcmp(x->passage,passage))continue;(*seq)++;
        printf("<sup class=\"aw-fn\"><a href=\"#n-%lld\" id=\"r-%lld\" aria-label=\"%s %d\">%d</a></sup>",x->id,x->id,!strcmp(x->kind,"footnote")?"Footnote":"Note",*seq,*seq);}
}
/* The passage text. Poems keep every line, blank lines and leading spaces;
 * prose becomes paragraphs. Units carry ids l-N or p-N for places. */
static void wl_text(const WlText *t,int pi,int marks){
    const char *body=t->p[pi].body;int poem=wl_is_poem(t->w.kind),unit=0,seq=0;
    if(poem){
        ui_open_a(stdout,"div","class","aw-verse","data-world-verse",(const char *)NULL);
        for(const char *p=body;*p||p==body;){
            const char *e=strchr(p,'\n');size_t len=e?(size_t)(e-p):strlen(p);unit++;
            int blank=strspn(p," \t")>=len;
            printf("<span class=\"aw-line%s\" id=\"l-%d\" data-unit=\"%d\">",blank?" aw-gap":"",unit,unit);
            if(unit%5==0&&!blank)printf("<span class=\"aw-ln\" aria-hidden=\"true\">%d</span>",unit);
            wl_inline(p,len);if(marks)wl_markers(t,t->p[pi].passage,unit,&seq);ui_close(stdout,"span"); fputs("\n",stdout);
            if(!e||!e[1])break;p=e+1;
        }
        ui_close(stdout,"div");return;
    }
    ui_open_a(stdout,"div","class","aw-prose","data-world-prose",(const char *)NULL);
    int open=0;
    for(const char *p=body;*p;){
        const char *e=strchr(p,'\n');size_t len=e?(size_t)(e-p):strlen(p);int blank=strspn(p," \t")>=len;
        if(blank){if(open){if(marks)wl_markers(t,t->p[pi].passage,unit,&seq);ui_close(stdout,"p");open=0;}}
        else{size_t lead=strspn(p," \t");if(!open){unit++;printf("<p id=\"p-%d\" data-unit=\"%d\">",unit,unit);open=1;}else fputc(' ',stdout);wl_inline(p+lead,len-lead);}
        if(!e)break;p=e+1;
    }
    if(open){if(marks)wl_markers(t,t->p[pi].passage,unit,&seq);ui_close(stdout,"p");}
    ui_close(stdout,"div");
}
static void wl_notes(const WlText *t,int pi){
    int seq=0,any=0;const char *passage=t->p[pi].passage;
    for(int g=0;g<t->ng;g++)if(!strcmp(t->g[g].passage,passage)){any=1;break;}
    if(!any)return;
    ui_open_a(stdout,"section","class","aw-notes","aria-labelledby","aw-notes-h"); ui_open_a(stdout,"h2","id","aw-notes-h","class","region-label"); fputs("Notes",stdout); ui_close(stdout,"h2"); ui_open(stdout,"ol",NULL);
    /* the same order the markers were numbered in: unit, then kind, then id */
    for(int g=0;g<t->ng;g++){const WlGloss *x=&t->g[g];if(strcmp(x->passage,passage))continue;seq++;
        printf("<li id=\"n-%lld\" value=\"%d\" data-gloss=\"%s\"><span class=\"aw-at\">%s %d</span> ",x->id,seq,x->kind,wl_is_poem(t->w.kind)?"Line":"Paragraph",x->unit);
        html(x->body);printf(" <a href=\"#r-%lld\" aria-label=\"Back to the text\">&#8617;</a></li>",x->id);}
    ui_close(stdout,"ol"); ui_close(stdout,"section");
}
static void wl_credit(const WlWork *w){
    ui_open(stdout,"p","by");html(w->author);if(w->translator[0]){fputs(", translated by ",stdout);html(w->translator);}ui_close(stdout,"p");
}
static void wl_terms(const WlWork *w){
    ui_open_a(stdout,"p","class","aw-terms",UI_DYN,"data-world-rights","",(w->rights));
    if(!strcmp(w->rights,"own")){fputs("Text &copy; ",stdout);html(w->author);fputs(". ",stdout);html(w->licence);}
    else html(w->licence);
    if(w->provenance[0]){fputs(" &middot; <a rel=\"noopener\" href=\"",stdout);html(w->provenance);fputs("\">",stdout);
        html(w->provenance_label[0]?w->provenance_label:"Where this text was taken from");ui_close(stdout,"a");}
    ui_close(stdout,"p");
}
/* Recitation: plays only under open terms its own source states now. */
static void wl_recitation(sqlite3 *db,const WlWork *w){
    if(!w->recitation[0])return;
    PageMaterial *m=calloc(1,sizeof(*m));PageItem *it=calloc(1,sizeof(*it));char loaded[96]="";
    int found=m&&it&&world_web_resolve(db,w->recitation,m,loaded,it);
    if(found&&wa_open_audio(it)){ui_open_a(stdout,"div","class","aw-recite","data-recitation","playing"); ui_open_a(stdout,"button","type","button","class","ak-btn is-primary","data-ak-track",(const char *)NULL,"data-ak-default",(const char *)NULL,UI_DYN,"data-src","",(it->asset),UI_DYN,"data-title","",(w->title),UI_DYN,"data-by","",(it->by[0]?it->by:it->title)); fputs("Hear it read",stdout); ui_close(stdout,"button"); ui_open(stdout,"p","aw-terms");html(it->title);if(it->by[0]){fputs(", ",stdout);html(it->by);}fputs(" &middot; ",stdout);html(it->rights);ui_close(stdout,"p"); ui_close(stdout,"div");}
    else if(found&&public_http_url(it->href)){ui_open_a(stdout,"p","class","aw-terms","data-recitation","linked"); ui_open_a(stdout,"a","rel","noopener",UI_DYN,"href","",(it->href)); fputs("Hear it read where the recording is published",stdout); ui_close(stdout,"a"); fputs("; its source states no open terms, so it does not play here.",stdout); ui_close(stdout,"p");}
    free(m);free(it);
}

/* ------------------------------------------------------------- space page */
static int wl_space_reads(sqlite3 *db,const char *ns,sqlite3_int64 world,const char *space){
    sqlite3_stmt *s=NULL;int any=awl_rows(db,NULL,ns,world,space,AWL_WORKS,NULL,&s)==1&&sqlite3_step(s)==SQLITE_ROW;sqlite3_finalize(s);return any;
}
static void wl_read_href(const char *space,const char *work,const char *passage){
    fputs("/worlds/",stdout);html(space);fputs("/read/",stdout);html(work);if(passage&&*passage){fputc('/',stdout);html(passage);}
}
/* The bibliography of a public space: every released work by collection. */
static void world_lit_public(sqlite3 *db,const PhSite *site,sqlite3_int64 world,const char *space){
    WlWork *w=NULL;int n=wl_works(db,NULL,site->id,world,space,&w);
    if(n){
        ui_open_a(stdout,"section","class","aw-works","aria-labelledby","aw-works-h","data-world-works",(const char *)NULL); ui_open_a(stdout,"h2","id","aw-works-h","class","region-label"); fputs("Writing",stdout); ui_close(stdout,"h2");
        char group[81]="\x01";
        for(int i=0;i<n;i++){
            if(strcmp(group,w[i].collection)){if(group[0]!='\x01')ui_close(stdout,"ol");snprintf(group,sizeof(group),"%s",w[i].collection);
                ui_open(stdout,"h3","aw-month");html(group[0]?group:"Works");ui_close(stdout,"h3"); ui_open(stdout,"ol","aw-bib");}
            fputs("<li data-work=\"",stdout);html(w[i].work);fputs("\"><a href=\"",stdout);wl_read_href(space,w[i].work,NULL);fputs("\">",stdout);html(w[i].title);
            ui_close(stdout,"a"); fputs(" ",stdout); ui_open(stdout,"span","aw-kind");html(wl_kind_word(w[i].kind));if(strcmp(w[i].language,"en")){fputs(" &middot; ",stdout);html(wl_language_word(w[i].language));}
            ui_close(stdout,"span"); ui_open(stdout,"span","by");html(w[i].author);if(w[i].translator[0]){fputs(", translated by ",stdout);html(w[i].translator);}ui_close(stdout,"span"); ui_close(stdout,"li");
        }
        ui_close(stdout,"ol"); ui_close(stdout,"section");
    }
    free(w);
}
/* A work named by an item "works/<key>": shown from its home space, never copied. */
static void world_lit_piece(sqlite3 *db,const PhSite *site,sqlite3_int64 world,int position,const char *ref,const char *role,const char *note){
    char home[65],work[65];WlText t;
    if(awl_public_key(db,site->id,world,ref+6,home,work)!=1||!wl_load(db,NULL,site->id,world,home,work,&t)){
        printf("<article class=\"aw-piece aw-gone\" id=\"piece-%d\" data-kind=\"gone\"><div class=\"aw-words\"><p class=\"aw-body\">This writing is not published right now.</p></div></article>",position);return;}
    char today[11];wl_today(today);int pi=-1;
    if(!strcmp(t.w.kind,"interactive"))pi=wl_first(&t);else for(int i=0;i<t.np&&pi<0;i++)if(wl_open(&t.p[i],today))pi=i;
    printf("<article class=\"aw-piece aw-work\" id=\"piece-%d\" data-kind=\"text\" data-world-piece=\"",position);html(ref);fputs("\" data-work-kind=\"",stdout);html(t.w.kind);
    fputs("\" data-shown=\"text\">",stdout); ui_open(stdout,"div","aw-words"); ui_open(stdout,"p","aw-role");world_web_words(role);fputs("</p><h2><a href=\"",stdout);wl_read_href(home,work,NULL);fputs("\">",stdout);html(t.w.title);ui_close(stdout,"a"); ui_close(stdout,"h2");
    wl_credit(&t.w);if(note&&*note){ui_text(stdout, "p", "aw-note", note);}
    wl_recitation(db,&t.w);
    if(pi>=0&&wl_is_poem(t.w.kind))wl_text(&t,pi,0);
    else if(pi>=0){ /* the opening paragraph, then the way in */
        const char *b=t.p[pi].body,*end=strstr(b,"\n\n");size_t len=end?(size_t)(end-b):strlen(b);if(len>700){len=700;while(len&&((unsigned char)b[len]&0xC0)==0x80)len--;}
        ui_open(stdout,"p","aw-body aw-opening");
        for(size_t i=0;i<len;i++){if(b[i]=='\n'){if(i+1<len)fputc(' ',stdout);}else wl_esc(b+i,1);}
        if(len==700)fputs("&hellip;",stdout);ui_close(stdout,"p");
    }
    fputs("<p class=\"aw-listen\"><a href=\"",stdout);wl_read_href(home,work,NULL);fputs("\">",stdout);
    fputs(wl_is_poem(t.w.kind)?"Open it in the reader":!strcmp(t.w.kind,"interactive")?"Begin the story":!strcmp(t.w.kind,"serial")?"Read the chapters":"Keep reading",stdout);
    ui_close(stdout,"a"); ui_close(stdout,"p");wl_terms(&t.w);ui_close(stdout,"div"); ui_close(stdout,"article");wl_free(&t);
}

/* ------------------------------------------------------------- reader */
static void wl_reader_bar(const PhSite *site,const char *space,const char *space_title,const char *work,const char *work_title){
    ui_open_a(stdout,"nav","class","aw-crumb","aria-label","Where you are"); ui_open_a(stdout,"a","class","home","href","/");html(site->name);ui_close(stdout,"a"); ui_open(stdout,"span","gr"); ui_open_a(stdout,"a",UI_DYN,"href","/worlds/",(space));
    html(space_title);ui_close(stdout,"a");if(work){fputs(" &rsaquo; <a href=\"",stdout);wl_read_href(space,work,NULL);fputs("\">",stdout);html(work_title);ui_close(stdout,"a");}ui_close(stdout,"span"); ui_close(stdout,"nav");
}
static void wl_controls(int poem){
    ui_open_a(stdout,"div","class","aw-readctl","role","group","aria-label","Reading settings","data-reader-controls",(const char *)NULL); ui_open_a(stdout,"button","type","button","data-size","-1","aria-label","Smaller text"); fputs("A&minus;",stdout); ui_close(stdout,"button"); ui_open_a(stdout,"button","type","button","data-size","1","aria-label","Larger text"); fputs("A+",stdout); ui_close(stdout,"button");
    if(poem)do{ ui_open_a(stdout,"button","type","button","data-numbers",(const char *)NULL,"aria-pressed","false"); fputs("Line numbers",stdout); ui_close(stdout,"button"); }while(0);
    ui_close(stdout,"div");
}
static void wl_place_link(const PhSite *site,const char *space,const char *work,const char *passage,const char *path,int poem){
    fputs("<p class=\"aw-place\"><a data-place data-unit-kind=\"",stdout);fputs(poem?"l":"p",stdout);fputs("\" href=\"https://aurekai.",stdout);html(site->territory);
    fputs("/workspace/worlds/reading?site=",stdout);url_query_value(site->id);fputs("&amp;space=",stdout);url_query_value(space);fputs("&amp;work=",stdout);url_query_value(work);
    fputs("&amp;passage=",stdout);url_query_value(passage);if(path&&*path){fputs("&amp;path=",stdout);url_query_value(path);}fputs("&amp;unit=0\">Keep my place in your account</a></p>",stdout);
}
static void wl_script(void){
    ui_open(stdout,"script",NULL); fputs("(()=>{const b=document.body,k='aw-read-size',get=n=>{try{return localStorage.getItem(n)}catch(e){return null}},put=(n,v)=>{try{localStorage.setItem(n,v)}catch(e){}};let size=+(get(k)||0);const apply=()=>{b.style.setProperty('--aw-read-step',size)};apply();document.querySelectorAll('[data-size]').forEach(x=>x.addEventListener('click',()=>{size=Math.max(-2,Math.min(4,size+ +x.dataset.size));put(k,size);apply()}));const num=document.querySelector('[data-numbers]');if(num){const on=get('aw-read-numbers')==='1';b.toggleAttribute('data-numbered',on);num.setAttribute('aria-pressed',on);num.addEventListener('click',()=>{const v=!b.hasAttribute('data-numbered');b.toggleAttribute('data-numbered',v);num.setAttribute('aria-pressed',v);put('aw-read-numbers',v?'1':'0')})}const keep=document.querySelector('[data-place]'),units=[...document.querySelectorAll('[data-unit]')],work=document.querySelector('[data-world-reader]');if(work){const store='aw-place:'+location.host+work.dataset.worldReader;let unit=0;const save=()=>{if(keep){const u=new URL(keep.href);u.searchParams.set('unit',unit);keep.href=u}put(store,JSON.stringify({href:location.pathname+location.search+(unit?'#'+(keep?keep.dataset.unitKind:'p')+'-'+unit:''),unit}))};if(units.length&&'IntersectionObserver' in window){const io=new IntersectionObserver(es=>{for(const e of es)if(e.isIntersecting){unit=+e.target.dataset.unit;save();break}},{rootMargin:'0px 0px -70% 0px'});units.forEach(u=>io.observe(u))}if(work.dataset.readerPage==='contents'){const back=document.querySelector('[data-continue]');let saved=null;try{saved=JSON.parse(get(store)||'null')}catch(e){}if(back&&saved&&saved.href){back.href=saved.href;back.hidden=false}}else save()}})()",stdout); ui_close(stdout,"script");
}
static int wl_bad(int code,const char *text){protocol_reply(code,"text/plain; charset=utf-8",text);return 1;}
/* "a.b.c" walks from the first passage along declared choices to passage. */
static int wl_path_ok(const WlText *t,const char *path,int target){
    char step[65];int prev=-1,count=0;const char *p=path;
    if(!path||!*path||strlen(path)>AWL_PATH_STEPS*65)return 0;
    for(;;){const char *dot=strchr(p,'.');size_t len=dot?(size_t)(dot-p):strlen(p);if(!len||len>64||++count>AWL_PATH_STEPS)return 0;
        memcpy(step,p,len);step[len]=0;int at=wl_find(t,step);if(at<0)return 0;
        if(prev<0){if(at!=wl_first(t))return 0;}
        else{int edge=0;for(int c=0;c<t->nc&&!edge;c++)edge=!strcmp(t->c[c].passage,t->p[prev].passage)&&!strcmp(t->c[c].target,step);if(!edge)return 0;}
        prev=at;if(!dot)break;p=dot+1;}
    return prev==target;
}
static void wl_path_from_parents(const WlText *t,const int *parent,int at,char *out,size_t cap){
    int chain[AWL_PATH_STEPS],n=0;for(int x=at;x>=0&&n<AWL_PATH_STEPS;x=parent[x])chain[n++]=x;
    size_t len=0;out[0]=0;for(int i=n-1;i>=0;i--){int w=snprintf(out+len,cap-len,"%s%s",len?".":"",t->p[chain[i]].passage);if(w<0||(size_t)w>=cap-len){out[0]=0;return;}len+=(size_t)w;}
}
static int serve_world_lit_public(const char *method,const char *target,const char *query,const char *host){
    const char *rest=target+8,*slash=strchr(rest,'/');char space[65],work[65]="",passage[65]="";
    if(!slash||slash==rest||(size_t)(slash-rest)>64||strncmp(slash,"/read/",6))return 0;
    memcpy(space,rest,(size_t)(slash-rest));space[slash-rest]=0;
    const char *w=slash+6,*ws=strchr(w,'/');size_t wl=ws?(size_t)(ws-w):strlen(w);
    if(!wl||wl>64)return 0;memcpy(work,w,wl);work[wl]=0;
    if(ws){if(!ws[1]||strlen(ws+1)>64||strchr(ws+1,'/'))return 0;snprintf(passage,sizeof(passage),"%s",ws+1);}
    const char *slugs="abcdefghijklmnopqrstuvwxyz0123456789-";
    if(strspn(space,slugs)!=strlen(space)||strspn(work,slugs)!=strlen(work)||strspn(passage,slugs)!=strlen(passage))return 0;
    char normalized[256];if(!lower_host(host,normalized)||host_is_territory_apex(host))return 0;
    sqlite3 *db=cms_open_ro();PhSite site;
    if(!db||!ph_site(db,normalized,NULL,&site)){if(db)sqlite3_close(db);return 0;}
    if(strcmp(method,"GET")&&strcmp(method,"HEAD")){sqlite3_close(db);aw_error(405,"Open the reader with GET.\n");return 1;}
    char path[AWL_PATH_STEPS*65+1]="";PhField params[]={{"path",path,sizeof(path),0}};
    if(query&&*query&&!ph_fields(query,params,1,sizeof(path)*3+16)){sqlite3_close(db);return wl_bad(400,"That reading path is not valid.\n");}
    sqlite3_stmt *row=NULL;
    if(aw_public_space(db,site.id,space,&row)!=1){sqlite3_close(db);return wl_bad(404,"There is no space at this address.\n");}
    char stitle[257],character[33],state[16];sqlite3_int64 world=sqlite3_column_int64(row,6);
    snprintf(stitle,sizeof(stitle),"%s",wl_col(row,0));snprintf(character,sizeof(character),"%s",wl_col(row,3));snprintf(state,sizeof(state),"%s",wl_col(row,5));
    sqlite3_finalize(row);
    if(strcmp(state,"public")){sqlite3_close(db);return wl_bad(410,"Its owner has taken this space down.\n");}
    if(!world_web_look(character))snprintf(character,sizeof(character),"journal");
    WlText t;if(!wl_load(db,NULL,site.id,world,space,work,&t)){sqlite3_close(db);return wl_bad(404,"There is no writing at this address.\n");}
    char today[11];wl_today(today);int interactive=!strcmp(t.w.kind,"interactive"),poem=wl_is_poem(t.w.kind),pi=-1;
    char *reach=calloc((size_t)t.np+1,1);int *parent=calloc((size_t)t.np+1,sizeof(int));
    if(!reach||!parent){free(reach);free(parent);wl_free(&t);sqlite3_close(db);aw_error(503,"Unavailable.\n");return 1;}
    int open_count=0,only=-1;for(int i=0;i<t.np;i++)if(wl_open(&t.p[i],today)){open_count++;only=i;}
    if(interactive){wl_reach(&t,reach,parent);pi=*passage?wl_find(&t,passage):wl_first(&t);if(pi>=0&&!reach[pi])pi=-1;
        if(pi>=0&&!wl_path_ok(&t,path,pi))wl_path_from_parents(&t,parent,pi,path,sizeof(path));}
    else{if(*passage){pi=wl_find(&t,passage);if(pi>=0&&!wl_open(&t.p[pi],today))pi=-1;}else if(open_count==1&&t.np==1)pi=only;*path=0;}
    if(*passage&&pi<0){free(reach);free(parent);wl_free(&t);sqlite3_close(db);return wl_bad(404,"That passage is not open to readers.\n");}
    if(interactive&&pi<0){free(reach);free(parent);wl_free(&t);sqlite3_close(db);return wl_bad(404,"This story has no passages yet.\n");}
    printf("HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nCache-Control: public, no-cache\r\nVary: Host\r\n"
           "X-Content-Type-Options: nosniff\r\nReferrer-Policy: strict-origin-when-cross-origin\r\nConnection: close\r\n\r\n");
    world_room_open(pi>=0&&t.p[pi].title[0]&&t.np>1?t.p[pi].title:t.w.title,site.name,character,"aw-reader",0);
    fputs("<main class=\"wrap aw-read\" data-world-reader=\"",stdout);
    wl_read_href(space,work,NULL);fputs("\" data-reader-page=\"",stdout);fputs(pi<0?"contents":"passage",stdout);fputs("\" data-work-kind=\"",stdout);html(t.w.kind);fputs("\" lang=\"",stdout);html(t.w.language);fputs("\">",stdout);
    wl_reader_bar(&site,space,stitle,pi>=0&&t.np>1?work:NULL,t.w.title);
    /* The monograph head: kicker, title, credit; what to hear on the right;
     * the work's facts in one strip beneath. */
    ui_open(stdout,"header","hero aw-readhead"); ui_open(stdout,"div","aw-readhead-main"); ui_open(stdout,"p","aw-role");html(wl_kind_word(t.w.kind));if(t.w.collection[0]){fputs(" &middot; ",stdout);html(t.w.collection);}
    ui_close(stdout,"p"); ui_open(stdout,"h1",NULL);html(t.w.title);ui_close(stdout,"h1");wl_credit(&t.w);
    if(pi>=0&&t.np>1&&t.p[pi].title[0]){ui_open(stdout,"h2","aw-chapter");if(!interactive)printf("<span>%d</span> ",t.p[pi].number);html(t.p[pi].title);ui_close(stdout,"h2");}
    ui_close(stdout,"div"); ui_open(stdout,"div","aw-readhead-acts");
    if(pi>=0){wl_recitation(db,&t.w);ui_open_a(stdout,"button","type","button","class","ak-btn","data-ak-track",(const char *)NULL,"data-narrate","#aw-text",UI_DYN,"data-title","",(t.w.title),UI_DYN,"data-by","",(t.w.author)); fputs("Read it aloud",stdout); ui_close(stdout,"button");}
    wl_controls(poem);
    ui_close(stdout,"div"); ui_open(stdout,"dl","aw-facts"); ui_open(stdout,"div",NULL); ui_text(stdout,"dt",NULL,"Form"); ui_open(stdout,"dd",NULL);html(wl_kind_word(t.w.kind));ui_close(stdout,"dd"); ui_close(stdout,"div"); ui_open(stdout,"div",NULL); ui_text(stdout,"dt",NULL,"Language"); ui_open(stdout,"dd",NULL);html(wl_language_word(t.w.language));
    printf("</dd></div><div><dt>%s</dt><dd>%d</dd></div><div><dt>Terms</dt><dd>",poem?"Parts":"Passages",t.np);
    if(!strcmp(t.w.rights,"own")){fputs("&copy; ",stdout);html(t.w.author);}else html(t.w.licence[0]?t.w.licence:t.w.rights);
    ui_close(stdout,"dd"); ui_close(stdout,"div"); ui_close(stdout,"dl"); ui_close(stdout,"header");
    if(pi<0){ /* contents */
        ui_open_a(stdout,"nav","class","aw-contents","aria-label","Contents"); ui_open(stdout,"p",NULL); ui_open_a(stdout,"a","class","aw-door","data-continue",(const char *)NULL,"hidden",(const char *)NULL,"href","#"); fputs("Continue where you left off",stdout); ui_close(stdout,"a"); ui_close(stdout,"p"); ui_open(stdout,"ol",NULL);
        int first_open=-1,next_wait=-1;
        for(int i=0;i<t.np;i++){int o=wl_open(&t.p[i],today);if(o&&first_open<0)first_open=i;
            printf("<li data-passage=\"%s\" data-open=\"%d\"><span class=\"aw-at\">%d</span> ",t.p[i].passage,o,t.p[i].number);
            if(o){fputs("<a href=\"",stdout);wl_read_href(space,work,t.p[i].passage);fputs("\">",stdout);html(t.p[i].title[0]?t.p[i].title:t.w.title);ui_close(stdout,"a");
                if(t.p[i].release_on[0]){fputs(" <time class=\"aw-when\" datetime=\"",stdout);html(t.p[i].release_on);fputs("\">",stdout);cine_when(t.p[i].release_on);ui_close(stdout,"time");}}
            else{if(next_wait<0)next_wait=i;html(t.p[i].title[0]?t.p[i].title:"A chapter to come");fputs(" <span class=\"aw-when\">Arrives <time datetime=\"",stdout);html(t.p[i].release_on);fputs("\">",stdout);cine_when(t.p[i].release_on);ui_close(stdout,"time"); ui_close(stdout,"span");}
            ui_close(stdout,"li");}
        ui_close(stdout,"ol");
        if(first_open>=0){fputs("<p><a class=\"aw-door\" href=\"",stdout);wl_read_href(space,work,t.p[first_open].passage);fputs("\">Start reading</a></p>",stdout);}
        else ui_text(stdout, "p", "aw-note", "Nothing is out yet.");
        ui_close(stdout,"nav");
    }else{
        ui_open_a(stdout,"article","class","aw-text","id","aw-text",UI_DYN,"data-passage","",(t.p[pi].passage));wl_text(&t,pi,1);ui_close(stdout,"article");
        if(interactive){
            int choices=0;for(int c=0;c<t.nc;c++)if(!strcmp(t.c[c].passage,t.p[pi].passage)){int to=wl_find(&t,t.c[c].target);if(to<0)continue;
                if(!choices++)do{ ui_open_a(stdout,"nav","class","aw-choices","aria-label","What happens next","data-world-choices",(const char *)NULL); ui_open(stdout,"ol",NULL); }while(0);
                fputs("<li><a href=\"",stdout);wl_read_href(space,work,t.c[c].target);fputs("?path=",stdout);
                char next[sizeof(path)+70];snprintf(next,sizeof(next),"%s.%s",path,t.c[c].target);url_query_value(next);fputs("\">",stdout);html(t.c[c].label);ui_close(stdout,"a"); ui_close(stdout,"li");}
            if(choices)do{ ui_close(stdout,"ol"); ui_close(stdout,"nav"); }while(0);
            else{fputs("<p class=\"aw-ending\" data-world-ending>This is one of the endings. <a href=\"",stdout);wl_read_href(space,work,NULL);fputs("\">Begin again</a></p>",stdout);}
            ui_open_a(stdout,"p","class","aw-terms aw-path",UI_DYN,"data-world-path","",(path)); fputs("Your way here: ",stdout);
            const char *p=path;int first=1;char step[65];
            while(*p){const char *dot=strchr(p,'.');size_t len=dot?(size_t)(dot-p):strlen(p);if(len>64)break;memcpy(step,p,len);step[len]=0;int at=wl_find(&t,step);
                if(!first)fputs(" &rarr; ",stdout);first=0;html(at>=0&&t.p[at].title[0]?t.p[at].title:step);if(!dot)break;p=dot+1;}
            ui_close(stdout,"p");
        }else if(t.np>1){
            int prev=-1,next=-1,wait=-1;for(int i=pi-1;i>=0&&prev<0;i--)if(wl_open(&t.p[i],today))prev=i;
            for(int i=pi+1;i<t.np&&next<0&&wait<0;i++){if(wl_open(&t.p[i],today))next=i;else wait=i;}
            ui_open_a(stdout,"nav","class","aw-turn","aria-label","Chapters");
            if(prev>=0){fputs("<a rel=\"prev\" href=\"",stdout);wl_read_href(space,work,t.p[prev].passage);fputs("\">&larr; ",stdout);html(t.p[prev].title[0]?t.p[prev].title:"Before");ui_close(stdout,"a");}
            fputs("<a href=\"",stdout);wl_read_href(space,work,NULL);fputs("\">Contents</a>",stdout);
            if(next>=0){fputs("<a rel=\"next\" href=\"",stdout);wl_read_href(space,work,t.p[next].passage);fputs("\">",stdout);html(t.p[next].title[0]?t.p[next].title:"Next");fputs(" &rarr;</a>",stdout);}
            else if(wait>=0){ui_open_a(stdout,"span","class","aw-when","data-next-release",(const char *)NULL); fputs("The next chapter arrives ",stdout); ui_open_a(stdout,"time",UI_DYN,"datetime","",(t.p[wait].release_on));cine_when(t.p[wait].release_on);ui_close(stdout,"time"); ui_close(stdout,"span");}
            ui_close(stdout,"nav");
        }
        wl_notes(&t,pi);
        wl_place_link(&site,space,work,t.p[pi].passage,path,poem);
    }
    /* other editions of the same text in this space */
    WlWork *all=NULL;int n=wl_works(db,NULL,site.id,world,space,&all),shown=0;
    for(int i=0;i<n;i++){WlWork *o=&all[i];if(!strcmp(o->work,t.w.work))continue;
        int related=!strcmp(o->original,t.w.work)||!strcmp(t.w.original,o->work)||(t.w.original[0]&&!strcmp(o->original,t.w.original));if(!related)continue;
        if(!shown++)do{ ui_open_a(stdout,"nav","class","aw-editions","aria-label","Other editions"); ui_open(stdout,"h2","region-label"); fputs("Other editions",stdout); ui_close(stdout,"h2"); ui_open(stdout,"ul",NULL); }while(0);
        fputs("<li><a href=\"",stdout);wl_read_href(space,o->work,NULL);fputs("\" hreflang=\"",stdout);html(o->language);fputs("\">",stdout);html(o->title);ui_close(stdout,"a"); fputs(" ",stdout); ui_open(stdout,"span",NULL);html(wl_language_word(o->language));
        if(!strcmp(t.w.original,o->work))fputs(", the original",stdout);else if(o->translator[0]){fputs(", translated by ",stdout);html(o->translator);}ui_close(stdout,"span"); ui_close(stdout,"li");}
    if(shown)do{ ui_close(stdout,"ul"); ui_close(stdout,"nav"); }while(0);free(all);
    ui_open(stdout,"div","src");wl_terms(&t.w);ui_close(stdout,"div");wl_script();world_room_close();
    free(reach);free(parent);wl_free(&t);sqlite3_close(db);return 1;
}

/* ------------------------------------------------------------- private API */
enum{WL_SITE,WL_WORLD,WL_SPACE,WL_REVISION,WL_OP,WL_RETURN,WL_WORK,WL_KIND,WL_TITLE,WL_AUTHOR,WL_TRANSLATOR,WL_LANGUAGE,WL_ORIGINAL,
     WL_COLLECTION,WL_RIGHTS,WL_LICENCE,WL_PROVENANCE,WL_SOURCE_LABEL,WL_RECITATION,WL_POSITION,WL_PASSAGE,WL_NUMBER,WL_RELEASE_ON,
     WL_START,WL_BODY,WL_LABEL,WL_TARGET,WL_UNIT,WL_NOTE_KIND,WL_ID,WL_PATH,WL_COUNT};
static int wl_int(const char *s,long long lo,long long hi,long long *out){sqlite3_int64 v=0;if(!world_web_number(s,&v,1)||v<lo||v>hi)return 0;*out=v;return 1;}
static void wl_json_work(bf_jsonw *w,const WlWork *x){
    const char *k[]={"work","key","kind","title","author","translator","language","original","collection","rights","licence","provenance","source_label","recitation"};
    const char *v[]={x->work,x->key,x->kind,x->title,x->author,x->translator,x->language,x->original,x->collection,x->rights,x->licence,x->provenance,x->provenance_label,x->recitation};
    bf_jsonw_obj(w);for(int i=0;i<14;i++){bf_jsonw_key(w,k[i]);bf_jsonw_str(w,v[i]);}bf_jsonw_key(w,"position");bf_jsonw_int(w,x->position);bf_jsonw_obj_end(w);
}
static void wl_reading_location(const char *site,const char *space){
    char location[300];snprintf(location,sizeof(location),"/workspace/worlds/reading?site=%s&space=%s",site,space);page_redirect(location);
}
/* The operations this owner takes, resolved once (strcmp order). */
#define WL_OPS_LIST(X) \
    X("choice", WLO_CHOICE) \
    X("draft", WLO_DRAFT) \
    X("gloss", WLO_GLOSS) \
    X("passage", WLO_PASSAGE) \
    X("place", WLO_PLACE) \
    X("places", WLO_PLACES) \
    X("unchoice", WLO_UNCHOICE) \
    X("ungloss", WLO_UNGLOSS) \
    X("unpassage", WLO_UNPASSAGE) \
    X("unplace", WLO_UNPLACE) \
    X("unwork", WLO_UNWORK) \
    X("work", WLO_WORK)
BF_COMMAND_TABLE(wl_ops, WL_OPS_LIST);
static int world_lit_api(const char *method,const char *query,const char *body,const char *raw,const char *host,const char *bearer,const char *auth_db){
    int post=!strcmp(method,"POST"),get=!strcmp(method,"GET");
    if(!post&&!get){aw_error(405,"Use GET or POST.\n");return 1;}
    if(post&&(!ph_origin(raw,host)||(query&&*query))){aw_error(403,"Submit from this workspace.\n");return 1;}
    static const char *names[WL_COUNT]={"site","world","space","revision","operation","return","work","kind","title","author","translator","language","original",
        "collection","rights","licence","provenance","source_label","recitation","position","passage","number","release_on","start","body","label","target","unit","note_kind","id","path"};
    static const size_t caps[WL_COUNT]={128,24,65,24,24,8,65,16,257,161,161,17,65,81,16,129,513,161,100,8,65,8,11,2,AWL_BODY_LIMIT+1,201,65,8,10,24,AWL_PATH_STEPS*65+1};
    size_t total=0;for(int i=0;i<WL_COUNT;i++)total+=caps[i];
    char *buf=calloc(1,total);if(!buf){aw_error(503,"Workspace unavailable.\n");return 1;}
    char *val[WL_COUNT];PhField fields[WL_COUNT];size_t at=0;
    for(int i=0;i<WL_COUNT;i++){val[i]=buf+at;at+=caps[i];fields[i].key=names[i];fields[i].value=val[i];fields[i].cap=caps[i];fields[i].multiline=i==WL_BODY;}
    sqlite3_int64 world=0,rev=0;const char *op;
    if(!ph_fields(post?body:query,fields,WL_COUNT,AWL_BODY_LIMIT*3+16384)||!*val[WL_SITE]||!*val[WL_SPACE]||
       (*val[WL_RETURN]&&strcmp(val[WL_RETURN],"studio")&&strcmp(val[WL_RETURN],"reading"))){free(buf);aw_error(400,"Invalid workspace request.\n");return 1;}
    op=val[WL_OP];
    int opid=bf_command_id(wl_ops,BF_COMMAND_COUNT(wl_ops),op,-1);
    int reader=opid==WLO_PLACE||opid==WLO_UNPLACE||opid==WLO_PLACES;
    if((!reader&&!world_web_number(val[WL_WORLD],&world,0))||(post&&!reader&&!world_web_number(val[WL_REVISION],&rev,0))||(get&&opid!=WLO_DRAFT&&opid!=WLO_PLACES)||
       (post&&opid==WLO_PLACES)||(reader&&!strcmp(val[WL_RETURN],"studio"))||(!reader&&!strcmp(val[WL_RETURN],"reading"))){free(buf);aw_error(400,"Invalid workspace request.\n");return 1;}
    /* text arrives with the browser's CRLF line ends; the owner keeps "\n" */
    {char *s=val[WL_BODY],*d=s;for(;*s;s++)if(*s!='\r')*d++=*s;*d=0;}
    const char *path=getenv("BONFYRE_CMS_DB");sqlite3 *db=NULL;char subject[160],again[160];const char *site=val[WL_SITE],*space=val[WL_SPACE];
    if(!path||!*path||!(db=post?cms_open_rw():cms_open_ro())){if(db)sqlite3_close(db);free(buf);aw_error(503,"Workspace unavailable.\n");return 1;}
    sqlite3_busy_timeout(db,2000);
    #define WL_ADMIT(out) (reader?cine_member_admit(bearer,auth_db,db,site,post,out):world_web_admit(bearer,auth_db,db,site,post,out))
    if(!WL_ADMIT(subject)){sqlite3_close(db);free(buf);aw_error(404,"Workspace unavailable.\n");return 1;}
    if(post&&awl_initialize(db)!=1){sqlite3_close(db);free(buf);aw_error(503,"Workspace unavailable.\n");return 1;}
    if(post&&(!WL_ADMIT(again)||strcmp(subject,again))){sqlite3_close(db);free(buf);aw_error(404,"Workspace unavailable.\n");return 1;}
    #undef WL_ADMIT
    int r=0,R=(int)rev;long long a=0,b=0;
    if(get){
        bf_jsonw w;bf_jsonw_init(&w,0);bf_jsonw_obj(&w);
        if(opid==WLO_PLACES){
            sqlite3_stmt *s=NULL;r=awl_places(db,subject,site,space,&s);
            bf_jsonw_key(&w,"places");bf_jsonw_arr(&w);
            while(r==1&&sqlite3_step(s)==SQLITE_ROW){bf_jsonw_obj(&w);const char *k[]={"work","passage","unit","path","updated"};
                for(int i=0;i<5;i++){bf_jsonw_key(&w,k[i]);if(i==2)bf_jsonw_int(&w,sqlite3_column_int64(s,i));else bf_jsonw_str(&w,wl_col(s,i));}bf_jsonw_obj_end(&w);}
            sqlite3_finalize(s);bf_jsonw_arr_end(&w);if(r==0)r=1;
        }else{
            sqlite3_stmt *probe=NULL;r=aw_space_get(db,subject,site,world,space,&probe);sqlite3_finalize(probe);
            if(r==1){bf_jsonw_key(&w,"state");bf_jsonw_str(&w,"draft");
                WlWork *works=NULL;int n=wl_works(db,subject,site,world,space,&works);bf_jsonw_key(&w,"works");bf_jsonw_arr(&w);for(int i=0;i<n;i++)wl_json_work(&w,&works[i]);bf_jsonw_arr_end(&w);free(works);
                static const char *pk[]={"work","passage","number","title","body","release_on","start"},*ck[]={"work","passage","number","label","target"},*gk[]={"id","work","passage","unit","kind","body"};
                const char **keys[]={NULL,pk,ck,gk};int counts[]={0,7,5,6};const char *lists[]={"","passages","choices","glosses"};
                for(int t=AWL_PASSAGES;t<=AWL_GLOSSES;t++){sqlite3_stmt *s=NULL;bf_jsonw_key(&w,lists[t]);bf_jsonw_arr(&w);
                    if(awl_rows(db,subject,site,world,space,t,NULL,&s)==1)while(sqlite3_step(s)==SQLITE_ROW){bf_jsonw_obj(&w);
                        for(int i=0;i<counts[t];i++){bf_jsonw_key(&w,keys[t][i]);if(sqlite3_column_type(s,i)==SQLITE_INTEGER)bf_jsonw_int(&w,sqlite3_column_int64(s,i));else bf_jsonw_str(&w,wl_col(s,i));}
                        bf_jsonw_obj_end(&w);}
                    sqlite3_finalize(s);bf_jsonw_arr_end(&w);}
            }
        }
        bf_jsonw_obj_end(&w);sqlite3_close(db);free(buf);
        if(r!=1)aw_error(r<0?503:404,"Workspace request could not be completed.\n");else if(w.oom)aw_error(503,"Workspace unavailable.\n");else aw_response(200,"application/json",w.buf,w.len,0);
        bf_jsonw_free(&w);return 1;
    }
    if(opid==WLO_WORK){if(wl_int(val[WL_POSITION],1,999,&a))r=awl_work_put(db,subject,site,world,space,val[WL_WORK],val[WL_KIND],val[WL_TITLE],val[WL_AUTHOR],val[WL_TRANSLATOR],
                              val[WL_LANGUAGE],val[WL_ORIGINAL],val[WL_COLLECTION],val[WL_RIGHTS],val[WL_LICENCE],val[WL_PROVENANCE],val[WL_SOURCE_LABEL],val[WL_RECITATION],(int)a,R);}
    else if(opid==WLO_UNWORK)r=awl_work_remove(db,subject,site,world,space,val[WL_WORK],R);
    else if(opid==WLO_PASSAGE){if(wl_int(val[WL_NUMBER],1,9999,&a)&&(!*val[WL_START]||!strcmp(val[WL_START],"1")))
        r=awl_passage_put(db,subject,site,world,space,val[WL_WORK],val[WL_PASSAGE],(int)a,val[WL_TITLE],val[WL_BODY],val[WL_RELEASE_ON],*val[WL_START]=='1',R);}
    else if(opid==WLO_UNPASSAGE)r=awl_passage_remove(db,subject,site,world,space,val[WL_WORK],val[WL_PASSAGE],R);
    else if(opid==WLO_CHOICE){if(wl_int(val[WL_NUMBER],1,AWL_CHOICE_LIMIT,&a))r=awl_choice_put(db,subject,site,world,space,val[WL_WORK],val[WL_PASSAGE],(int)a,val[WL_LABEL],val[WL_TARGET],R);}
    else if(opid==WLO_UNCHOICE){if(wl_int(val[WL_NUMBER],1,AWL_CHOICE_LIMIT,&a))r=awl_choice_remove(db,subject,site,world,space,val[WL_WORK],val[WL_PASSAGE],(int)a,R);}
    else if(opid==WLO_GLOSS){if(wl_int(val[WL_UNIT],1,100000,&a))r=awl_gloss_put(db,subject,site,world,space,val[WL_WORK],val[WL_PASSAGE],(int)a,val[WL_NOTE_KIND],val[WL_LABEL],R);}
    else if(opid==WLO_UNGLOSS){if(wl_int(val[WL_ID],1,INT64_MAX/2,&b))r=awl_gloss_remove(db,subject,site,world,space,b,R);}
    else if(opid==WLO_PLACE){if(wl_int(val[WL_UNIT],0,100000,&a))r=awl_place_put(db,subject,site,space,val[WL_WORK],val[WL_PASSAGE],(int)a,val[WL_PATH]);}
    else if(opid==WLO_UNPLACE)r=awl_place_clear(db,subject,site,space,val[WL_WORK]);
    else{sqlite3_close(db);free(buf);aw_error(400,"Invalid workspace request.\n");return 1;}
    sqlite3_close(db);
    if(r!=1){free(buf);aw_error(r<0?503:409,"Workspace request could not be completed.\n");return 1;}
    if(!strcmp(val[WL_RETURN],"studio")){world_web_studio_location(site,world,space);free(buf);return 1;}
    if(!strcmp(val[WL_RETURN],"reading")){wl_reading_location(site,space);free(buf);return 1;}
    free(buf);aw_response(200,"application/json","{\"state\":\"saved\"}",17,0);return 1;
}

/* ------------------------------------------------------------- reading room */
static int serve_world_reading(const char *method,const char *query,const char *bearer,const char *auth_db){
    if(strcmp(method,"GET")){aw_error(405,"Open your reading with GET.\n");return 1;}
    char site_id[128]="",space[65]="",work[65]="",passage[65]="",unit_s[12]="",path[AWL_PATH_STEPS*65+1]="";
    PhField params[]={{"site",site_id,sizeof(site_id),0},{"space",space,sizeof(space),0},{"work",work,sizeof(work),0},{"passage",passage,sizeof(passage),0},
                      {"unit",unit_s,sizeof(unit_s),0},{"path",path,sizeof(path),0}};
    long long unit=0;
    if(!ph_fields(query,params,6,sizeof(path)*3+2048)||!*site_id||!*space||(*unit_s&&!wl_int(unit_s,0,100000,&unit))){aw_error(400,"Choose a space to read.\n");return 1;}
    sqlite3 *db=cms_open_ro();char viewer[160];PhSite site;sqlite3_stmt *row=NULL;
    if(!db||!cine_member_admit(bearer,auth_db,db,site_id,0,viewer)||!ph_site(db,NULL,site_id,&site)||aw_public_space(db,site_id,space,&row)!=1||strcmp(wl_col(row,5),"public")){
        sqlite3_finalize(row);if(db)sqlite3_close(db);aw_error(404,"Sign in to keep your place in a published space.\n");return 1;}
    char title[257],character[33];sqlite3_int64 world=sqlite3_column_int64(row,6);snprintf(title,sizeof(title),"%s",wl_col(row,0));snprintf(character,sizeof(character),"%s",wl_col(row,3));
    sqlite3_finalize(row);if(!world_web_look(character))snprintf(character,sizeof(character),"journal");
    fputs("HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nCache-Control: private, no-store\r\nVary: Cookie, Authorization\r\n"
          "X-Content-Type-Options: nosniff\r\nReferrer-Policy: same-origin\r\nConnection: close\r\n\r\n",stdout);
    world_room_open(title,"your reading",character,"aw-clubroom aw-readingroom",1);
    ui_open_a(stdout,"main","class","wrap aw-room",UI_DYN,"data-world-reading","",(space));
    fputs("<nav class=\"aw-crumb\" aria-label=\"Where you are\"><a class=\"home\" href=\"https://",stdout);html(site.host);fputs("/worlds/",stdout);html(space);fputs("\">",stdout);html(title);
    ui_close(stdout,"a"); ui_open(stdout,"span","ch"); fputs("your reading",stdout); ui_close(stdout,"span"); ui_close(stdout,"nav"); ui_open(stdout,"header","hero"); ui_open(stdout,"h1",NULL); fputs("Your place in ",stdout);html(title);
    ui_close(stdout,"h1"); ui_text(stdout,"p","lede","Where you stopped in each piece, kept with your account so you can pick up on any device. Only you can see this page."); ui_close(stdout,"header");
    WlWork *works=NULL;int nw=wl_works(db,NULL,site_id,world,space,&works);
    if(*work){ /* confirm before keeping */
        WlText t;
        if(wl_load(db,NULL,site_id,world,space,work,&t)){int pi=wl_find(&t,passage);
            ui_open_a(stdout,"section","class","aw-join","data-reading-keep",(const char *)NULL); ui_open(stdout,"h2","region-label"); fputs("Keep this place",stdout); ui_close(stdout,"h2"); ui_open(stdout,"p","aw-note");html(t.w.title);
            if(pi>=0&&t.np>1&&t.p[pi].title[0]){fputs(", ",stdout);html(t.p[pi].title);}
            if(unit>0)printf(", %s %lld",wl_is_poem(t.w.kind)?"line":"paragraph",unit);ui_close(stdout,"p");
            ui_open_a(stdout,"form","method","post","action","/workspace/world-literature");ph_hidden("site",site_id);ph_hidden("space",space);ph_hidden("operation","place");
            ph_hidden("work",work);ph_hidden("passage",passage);char u[24];snprintf(u,sizeof(u),"%lld",unit);ph_hidden("unit",u);ph_hidden("path",path);ph_hidden("return","reading");
            ui_open_a(stdout,"button","type","submit"); fputs("Keep my place",stdout); ui_close(stdout,"button"); ui_close(stdout,"form"); ui_close(stdout,"section");wl_free(&t);}
    }
    ui_open_a(stdout,"section","class","aw-picks","aria-labelledby","aw-places-h"); ui_open_a(stdout,"h2","id","aw-places-h","class","region-label"); fputs("Where you are reading",stdout); ui_close(stdout,"h2"); ui_open_a(stdout,"ol","data-reading-places",(const char *)NULL);
    sqlite3_stmt *s=NULL;int have=0;
    if(awl_places(db,viewer,site_id,space,&s)==1)while(sqlite3_step(s)==SQLITE_ROW){
        const char *w=wl_col(s,0),*p=wl_col(s,1),*pa=wl_col(s,3);int u=sqlite3_column_int(s,2);const WlWork *x=NULL;
        for(int i=0;i<nw;i++)if(!strcmp(works[i].work,w))x=&works[i];if(!x)continue;have++;
        fputs("<li data-reading-place=\"",stdout);html(w);fputs("\"><div class=\"aw-words\"><h3><a href=\"https://",stdout);html(site.host);wl_read_href(space,w,p);
        if(*pa){fputs("?path=",stdout);url_query_value(pa);}if(u)printf("#%s-%d",wl_is_poem(x->kind)?"l":"p",u);fputs("\">",stdout);html(x->title);ui_close(stdout,"a"); ui_close(stdout,"h3"); ui_open(stdout,"p","by");
        if(u)printf("%s %d &middot; ",wl_is_poem(x->kind)?"Line":"Paragraph",u);fputs("kept ",stdout);cine_when(wl_col(s,4));ui_close(stdout,"p"); ui_close(stdout,"div"); ui_open(stdout,"div","aw-acts");
        ui_open_a(stdout,"form","method","post","action","/workspace/world-literature");ph_hidden("site",site_id);ph_hidden("space",space);ph_hidden("operation","unplace");ph_hidden("work",w);ph_hidden("return","reading");
        ui_open_a(stdout,"button","type","submit","class","aw-quiet"); fputs("Forget",stdout); ui_close(stdout,"button"); ui_close(stdout,"form"); ui_close(stdout,"div"); ui_close(stdout,"li");
    }
    sqlite3_finalize(s);if(!have)do{ ui_open(stdout,"li",NULL); ui_text(stdout,"p","aw-body","You have not kept a place here yet."); ui_close(stdout,"li"); }while(0);
    ui_close(stdout,"ol"); ui_close(stdout,"section");world_room_close();free(works);sqlite3_close(db);return 1;
}

/* ------------------------------------------------------------- studio */
static void wl_form(const char *site,sqlite3_int64 world,const char *space,const char *op,long long revision,const char *name){
    fputs("<form method=\"post\" action=\"/workspace/world-literature\"",stdout);if(name){fputs(" data-lit-form=\"",stdout);html(name);fputs("\"",stdout);}fputs(">",stdout);
    char n[24];ph_hidden("site",site);snprintf(n,sizeof(n),"%lld",(long long)world);ph_hidden("world",n);ph_hidden("space",space);ph_hidden("operation",op);
    snprintf(n,sizeof(n),"%lld",revision);ph_hidden("revision",n);ph_hidden("return","studio");
}
/* Earlier text against the current one, line by line (longest common run). */
static void wl_diff(const char *old,const char *now){
    enum{MAXL=600};const char *a[MAXL],*b[MAXL];size_t al[MAXL],bl[MAXL];int na=0,nb=0;
    for(const char *p=old;*p&&na<MAXL;){const char *e=strchr(p,'\n');a[na]=p;al[na++]=e?(size_t)(e-p):strlen(p);if(!e)break;p=e+1;}
    for(const char *p=now;*p&&nb<MAXL;){const char *e=strchr(p,'\n');b[nb]=p;bl[nb++]=e?(size_t)(e-p):strlen(p);if(!e)break;p=e+1;}
    if(na>=MAXL||nb>=MAXL){ui_text(stdout, "p", "aw-terms", "Too long to compare line by line here.");return;}
    short *L=calloc((size_t)(na+1)*(size_t)(nb+1),sizeof(short));if(!L)return;
    #define LL(i,j) L[(size_t)(i)*(size_t)(nb+1)+(size_t)(j)]
    for(int i=na-1;i>=0;i--)for(int j=nb-1;j>=0;j--)LL(i,j)=al[i]==bl[j]&&!memcmp(a[i],b[j],al[i])?(short)(LL(i+1,j+1)+1):(LL(i+1,j)>=LL(i,j+1)?LL(i+1,j):LL(i,j+1));
    ui_open_a(stdout,"ol","class","aw-diff","data-lit-diff",(const char *)NULL);int i=0,j=0;
    while(i<na||j<nb){
        if(i<na&&j<nb&&al[i]==bl[j]&&!memcmp(a[i],b[j],al[i])){ui_open(stdout,"li",NULL);wl_esc(a[i],al[i]);ui_close(stdout,"li");i++;j++;}
        else if(j<nb&&(i>=na||LL(i,j+1)>=LL(i+1,j))){ui_open(stdout,"li","ins"); ui_open(stdout,"ins",NULL);wl_esc(b[j],bl[j]);ui_close(stdout,"ins"); ui_close(stdout,"li");j++;}
        else{ui_open(stdout,"li","del"); ui_open(stdout,"del",NULL);wl_esc(a[i],al[i]);ui_close(stdout,"del"); ui_close(stdout,"li");i++;}
    }
    #undef LL
    ui_close(stdout,"ol");free(L);
}
static void wl_work_fields(const WlWork *w,const WlWork *all,int n,const PhSite *site){
    const char *kinds[]={"poem","story","essay","memoir","serial","interactive"};
    ui_open(stdout,"div","aw-row"); ui_open(stdout,"label",NULL); fputs("Title",stdout); ui_open_a(stdout,"input","name","title","required",(const char *)NULL,"maxlength","256",UI_DYN,"value","",(w?w->title:"")); ui_close(stdout,"label"); ui_open(stdout,"label",NULL); fputs("What it is",stdout); ui_open_a(stdout,"select","name","kind");
    for(int i=0;i<6;i++){fputs("<option value=\"",stdout);fputs(kinds[i],stdout);fputs("\"",stdout);if(w&&!strcmp(w->kind,kinds[i]))fputs(" selected",stdout);fputs(">",stdout);html(wl_kind_word(kinds[i]));ui_close(stdout,"option");}
    fputs("</select></label></div><div class=\"aw-row\"><label>Written by<input name=\"author\" required maxlength=\"160\" value=\"",stdout);html(w?w->author:site->name);
    fputs("\"></label><label>Translated by<small>Optional</small><input name=\"translator\" maxlength=\"160\" value=\"",stdout);html(w?w->translator:"");
    fputs("\"></label><label>Language<small>A code such as en or es</small><input name=\"language\" required maxlength=\"16\" pattern=\"[A-Za-z-]{2,16}\" value=\"",stdout);html(w?w->language:"en");
    fputs("\"></label></div><div class=\"aw-row\"><label>Collection<small>Optional. Works are grouped under it</small><input name=\"collection\" maxlength=\"80\" value=\"",stdout);html(w?w->collection:"");
    fputs("\">",stdout); ui_close(stdout,"label"); ui_open(stdout,"label",NULL); fputs("Translation of",stdout); ui_open(stdout,"small",NULL); fputs("Optional",stdout); ui_close(stdout,"small"); ui_open_a(stdout,"select","name","original"); ui_open_a(stdout,"option","value",""); fputs("Not a translation",stdout); ui_close(stdout,"option");
    for(int i=0;i<n;i++){if(w&&!strcmp(all[i].work,w->work))continue;fputs("<option value=\"",stdout);html(all[i].work);fputs("\"",stdout);if(w&&!strcmp(w->original,all[i].work))fputs(" selected",stdout);fputs(">",stdout);html(all[i].title);ui_close(stdout,"option");}
    char pos[8];snprintf(pos,sizeof(pos),"%d",w?w->position:n+1);
    ui_close(stdout,"select"); ui_close(stdout,"label"); ui_open(stdout,"label",NULL); fputs("Order",stdout); ui_open_a(stdout,"input","type","number","name","position","min","1","max","999","required",(const char *)NULL,UI_DYN,"value","",(pos)); ui_close(stdout,"label"); ui_close(stdout,"div");
    fputs("<div class=\"aw-row\"><label>Whose text<select name=\"rights\"><option value=\"own\"",stdout);if(!w||!strcmp(w->rights,"own"))fputs(" selected",stdout);
    fputs(">My own writing</option><option value=\"public-domain\"",stdout);if(w&&!strcmp(w->rights,"public-domain"))fputs(" selected",stdout);
    fputs(">A public-domain text</option></select></label><label>Terms<small>Yours, such as All rights reserved or CC BY 4.0. For a public-domain text, begin with Public domain</small><input name=\"licence\" required maxlength=\"128\" value=\"",stdout);
    html(w?w->licence:"All rights reserved");fputs("\"></label></div><div class=\"aw-row\"><label>Taken from<small>Required for a public-domain text: the https page, such as Wikisource or Project Gutenberg</small><input name=\"provenance\" maxlength=\"512\" value=\"",stdout);
    html(w?w->provenance:"");fputs("\"></label><label>Name of that page<small>Optional</small><input name=\"source_label\" maxlength=\"160\" value=\"",stdout);html(w?w->provenance_label:"");
    fputs("\"></label><label>Recording of it read aloud<small>Optional. The address after /o/ of the recording. It plays only if its source states open terms</small><input name=\"recitation\" maxlength=\"99\" value=\"",stdout);
    html(w?w->recitation:"");fputs("\">",stdout); ui_close(stdout,"label"); ui_close(stdout,"div");
}
static void world_lit_studio(sqlite3 *db,const char *subject,const char *site_id,sqlite3_int64 world,const char *space,long long revision){
    PhSite site;ph_site(db,NULL,site_id,&site);WlWork *works=NULL;int n=wl_works(db,subject,site_id,world,space,&works);

    ui_open_a(stdout,"section","id","writing","class","aw-lit-studio","data-world-lit-studio",(const char *)NULL); ui_open(stdout,"h3",NULL); fputs("Writing",stdout); ui_close(stdout,"h3"); ui_open(stdout,"p",NULL); fputs("Poems, stories, essays, memoir, serials told in chapters, and stories readers steer by choosing. Text is kept exactly as you type it: line breaks, indents and blank lines between stanzas. Put _underscores_ around words to set them in italics. Readers see a work once it is on a published space; a chapter with a date stays hidden until that day.",stdout); ui_close(stdout,"p");
    /* which works are already placed on this page */
    char placed[AWL_WORK_LIMIT]={0};int next_item=1;sqlite3_stmt *items=NULL;
    if(aw_items(db,subject,site_id,world,space,&items)==1)while(sqlite3_step(items)==SQLITE_ROW){int pos=sqlite3_column_int(items,0);const char *ref=wl_col(items,1);if(pos>=next_item)next_item=pos+1;
        for(int i=0;i<n;i++)if(!strncmp(ref,"works/",6)&&!strcmp(ref+6,works[i].key))placed[i]=1;}
    sqlite3_finalize(items);
    for(int i=0;i<n;i++){
        WlText t;if(!wl_load(db,subject,site_id,world,space,works[i].work,&t))continue;const WlWork *w=&t.w;int inter=!strcmp(w->kind,"interactive");
        ui_open_a(stdout,"details",UI_DYN,"data-lit-work","",(w->work)); ui_open(stdout,"summary",NULL);html(w->title);fputs(" &middot; ",stdout);html(wl_kind_word(w->kind));
        printf(" &middot; %d passage%s",t.np,t.np==1?"":"s");if(placed[i])fputs(" &middot; on this page",stdout);ui_close(stdout,"summary");
        wl_form(site_id,world,space,"work",revision,"work");ph_hidden("work",w->work);wl_work_fields(w,works,n,&site);
        ui_open(stdout,"div","aw-actions"); ui_open_a(stdout,"button","type","submit"); fputs("Save details",stdout); ui_close(stdout,"button"); ui_close(stdout,"div"); ui_close(stdout,"form");
        ui_open(stdout,"div","aw-actions");
        if(!placed[i]&&next_item<=AW_ITEM_LIMIT){ui_open_a(stdout,"form","method","post","action","/workspace/world","data-lit-form","place");world_web_hidden_base(site_id,world,space,"item",revision);
            char p[8],ref[40];snprintf(p,sizeof(p),"%d",next_item);snprintf(ref,sizeof(ref),"works/%s",w->key);ph_hidden("position",p);ph_hidden("ref",ref);ph_hidden("role",wl_is_poem(w->kind)?"poem":w->kind);
            ui_open_a(stdout,"button","type","submit","class","aw-quiet"); fputs("Show it on this page",stdout); ui_close(stdout,"button"); ui_close(stdout,"form");}
        ui_open(stdout,"span","aw-terms"); fputs("To show it in another of your spaces, add the piece ",stdout); ui_open(stdout,"code",NULL); fputs("works/",stdout);html(w->key);ui_close(stdout,"code"); fputs(" there.",stdout); ui_close(stdout,"span"); ui_close(stdout,"div");
        /* passages */
        char *reach=calloc((size_t)t.np+1,1);int *parent=calloc((size_t)t.np+1,sizeof(int));if(inter&&reach&&parent)wl_reach(&t,reach,parent);
        for(int p=0;p<t.np;p++){const WlPassage *x=&t.p[p];
            printf("<details data-lit-passage=\"%s\"><summary>%d. ",x->passage,x->number);html(x->title[0]?x->title:x->passage);
            if(x->release_on[0]){fputs(" &middot; out ",stdout);cine_when(x->release_on);}if(x->start)fputs(" &middot; first passage",stdout);
            if(inter&&reach&&!reach[p])fputs(" &middot; <span class=\"aw-warn\">readers cannot reach this yet</span>",stdout);ui_close(stdout,"summary");
            wl_form(site_id,world,space,"passage",revision,"passage");ph_hidden("work",w->work);ph_hidden("passage",x->passage);
            char num[8];snprintf(num,sizeof(num),"%d",x->number);
            fputs("<div class=\"aw-row\"><label>Number<input type=\"number\" name=\"number\" min=\"1\" max=\"9999\" required value=\"",stdout);html(num);
            fputs("\"></label><label>Title<small>Optional for a single poem</small><input name=\"title\" maxlength=\"256\" value=\"",stdout);html(x->title);
            fputs("\"></label><label>Out on<small>Optional. Hidden from readers until this day</small><input type=\"date\" name=\"release_on\" value=\"",stdout);html(x->release_on);fputs("\"></label></div>",stdout);
            ui_open(stdout,"label",NULL); fputs("Text",stdout); ui_open_a(stdout,"textarea","class","aw-text","name","body","rows","12","required",(const char *)NULL,"maxlength","60000","spellcheck","true"); fputs("\n",stdout);html(x->body);ui_close(stdout,"textarea"); ui_close(stdout,"label");
            if(inter){fputs("<label class=\"aw-check\"><input type=\"checkbox\" name=\"start\" value=\"1\"",stdout);if(x->start)fputs(" checked",stdout);fputs("> Readers begin here</label>",stdout);}
            ui_open(stdout,"div","aw-actions"); ui_open_a(stdout,"button","type","submit"); fputs("Save this text",stdout); ui_close(stdout,"button"); ui_close(stdout,"div"); ui_close(stdout,"form");
            if(inter){int nc=0,used[AWL_CHOICE_LIMIT+1]={0};
                for(int c=0;c<t.nc;c++)if(!strcmp(t.c[c].passage,x->passage)){used[t.c[c].number]=1;
                    if(!nc++)ui_open_a(stdout,"ol","class","aw-items","data-lit-choices",(const char *)NULL);int to=wl_find(&t,t.c[c].target);
                    ui_open(stdout,"li",NULL); ui_open(stdout,"div",NULL);html(t.c[c].label);fputs(" &rarr; ",stdout);html(to>=0&&t.p[to].title[0]?t.p[to].title:t.c[c].target);ui_close(stdout,"div");
                    wl_form(site_id,world,space,"unchoice",revision,NULL);ph_hidden("work",w->work);ph_hidden("passage",x->passage);char cn[4];snprintf(cn,sizeof(cn),"%d",t.c[c].number);ph_hidden("number",cn);
                    ui_open_a(stdout,"button","type","submit","class","aw-quiet"); fputs("Remove",stdout); ui_close(stdout,"button"); ui_close(stdout,"form"); ui_close(stdout,"li");}
                if(nc)ui_close(stdout,"ol");else ui_text(stdout, "p", NULL, "No choices: this passage is an ending.");
                int free_n=0;for(int k=1;k<=AWL_CHOICE_LIMIT&&!free_n;k++)if(!used[k])free_n=k;
                if(free_n){wl_form(site_id,world,space,"choice",revision,"choice");ph_hidden("work",w->work);ph_hidden("passage",x->passage);char cn[4];snprintf(cn,sizeof(cn),"%d",free_n);ph_hidden("number",cn);
                    ui_open(stdout,"div","aw-row"); ui_open(stdout,"label",NULL); fputs("A choice readers can make",stdout); ui_open_a(stdout,"input","name","label","required",(const char *)NULL,"maxlength","200"); ui_close(stdout,"label"); ui_open(stdout,"label",NULL); fputs("Leads to",stdout); ui_open_a(stdout,"select","name","target");
                    for(int q=0;q<t.np;q++){ui_open_a(stdout,"option",UI_DYN,"value","",(t.p[q].passage));html(t.p[q].title[0]?t.p[q].title:t.p[q].passage);ui_close(stdout,"option");}
                    ui_close(stdout,"select"); ui_close(stdout,"label"); ui_close(stdout,"div"); ui_open(stdout,"div","aw-actions"); ui_open_a(stdout,"button","type","submit"); fputs("Add the choice",stdout); ui_close(stdout,"button"); ui_close(stdout,"div"); ui_close(stdout,"form");}}
            /* glosses on this passage */
            int ng=0;for(int g=0;g<t.ng;g++)if(!strcmp(t.g[g].passage,x->passage)){if(!ng++)ui_open_a(stdout,"ol","class","aw-items","data-lit-glosses",(const char *)NULL);
                printf("<li><div><strong>%s %s %d</strong><br>",!strcmp(t.g[g].kind,"footnote")?"Footnote,":"Note,",wl_is_poem(w->kind)?"line":"paragraph",t.g[g].unit);html(t.g[g].body);ui_close(stdout,"div");
                wl_form(site_id,world,space,"ungloss",revision,NULL);char id[24];snprintf(id,sizeof(id),"%lld",t.g[g].id);ph_hidden("id",id);ui_open_a(stdout,"button","type","submit","class","aw-quiet"); fputs("Remove",stdout); ui_close(stdout,"button"); ui_close(stdout,"form"); ui_close(stdout,"li");}
            if(ng)ui_close(stdout,"ol");
            wl_form(site_id,world,space,"gloss",revision,"gloss");ph_hidden("work",w->work);ph_hidden("passage",x->passage);
            printf("<div class=\"aw-row\"><label>%s<input type=\"number\" name=\"unit\" min=\"1\" max=\"%d\" required value=\"1\"></label>",wl_is_poem(w->kind)?"On line":"On paragraph",awl_units(w->kind,x->body));
            ui_open(stdout,"label",NULL); fputs("Kind",stdout); ui_open_a(stdout,"select","name","note_kind"); ui_open_a(stdout,"option","value","footnote"); fputs("Footnote",stdout); ui_close(stdout,"option"); ui_open_a(stdout,"option","value","note"); fputs("Note",stdout); ui_close(stdout,"option"); ui_close(stdout,"select"); ui_close(stdout,"label"); ui_close(stdout,"div"); ui_open(stdout,"label",NULL); fputs("The note",stdout); ui_open_a(stdout,"input","name","label","required",(const char *)NULL,"maxlength","1000"); ui_close(stdout,"label"); ui_open(stdout,"div","aw-actions"); ui_open_a(stdout,"button","type","submit","class","aw-quiet"); fputs("Add the note",stdout); ui_close(stdout,"button"); ui_close(stdout,"div"); ui_close(stdout,"form");
            /* earlier versions */
            sqlite3_stmt *v=NULL;int nv=0;
            if(awl_versions(db,subject,site_id,world,space,w->work,x->passage,&v)==1)while(sqlite3_step(v)==SQLITE_ROW){
                if(!nv++)do{ ui_open_a(stdout,"details","data-lit-versions",(const char *)NULL); ui_open(stdout,"summary",NULL); fputs("Earlier versions",stdout); ui_close(stdout,"summary"); }while(0);
                printf("<p class=\"aw-state\">Before change %d, compared with the text now</p>",sqlite3_column_int(v,0));wl_diff(wl_col(v,1),x->body);}
            sqlite3_finalize(v);if(nv)ui_close(stdout,"details");
            wl_form(site_id,world,space,"unpassage",revision,NULL);ph_hidden("work",w->work);ph_hidden("passage",x->passage);ui_open_a(stdout,"button","type","submit","class","aw-quiet"); fputs("Remove this passage",stdout); ui_close(stdout,"button"); ui_close(stdout,"form"); ui_close(stdout,"details");
        }
        if(inter&&t.np){int endings=0,lost=0;for(int p=0;p<t.np;p++){int out=0;for(int c=0;c<t.nc;c++)if(!strcmp(t.c[c].passage,t.p[p].passage))out++;if(!out&&reach&&reach[p])endings++;if(reach&&!reach[p])lost++;}
            printf("<p data-lit-structure>%d ending%s readers can reach.",endings,endings==1?"":"s");if(lost)printf(" <span class=\"aw-warn\">%d passage%s cannot be reached from the first one.</span>",lost,lost==1?"":"s");ui_close(stdout,"p");}
        free(reach);free(parent);
        /* new passage */
        int next=1;for(int p=0;p<t.np;p++)if(t.p[p].number>=next)next=t.p[p].number+1;
        if(t.np<AWL_PASSAGE_LIMIT){wl_form(site_id,world,space,"passage",revision,"new-passage");ph_hidden("work",w->work);char num[8];snprintf(num,sizeof(num),"%d",next);
            ui_open(stdout,"h4",NULL);fputs(!strcmp(w->kind,"serial")?"Next chapter":inter?"New passage":t.np?"Another part":"The text",stdout);fputs("</h4><div class=\"aw-row\"><label>Short name<small>Lowercase, used in the link</small><input name=\"passage\" required maxlength=\"64\" pattern=\"[a-z0-9][a-z0-9-]*\" value=\"",stdout);
            if(!t.np)fputs("text",stdout);fputs("\"></label><label>Number<input type=\"number\" name=\"number\" min=\"1\" max=\"9999\" required value=\"",stdout);html(num);
            fputs("\"></label><label>Title<small>Optional</small><input name=\"title\" maxlength=\"256\"></label><label>Out on<small>Optional</small><input type=\"date\" name=\"release_on\"></label></div>"
                  "<label>Text<textarea class=\"aw-text\" name=\"body\" rows=\"10\" required maxlength=\"60000\"></textarea></label>",stdout);
            if(inter&&!t.np)ui_open_a(stdout,"input","type","hidden","name","start","value","1");
            ui_open(stdout,"div","aw-actions"); ui_open_a(stdout,"button","type","submit"); fputs("Add",stdout); ui_close(stdout,"button"); ui_close(stdout,"div"); ui_close(stdout,"form");}
        wl_form(site_id,world,space,"unwork",revision,NULL);ph_hidden("work",w->work);ui_open_a(stdout,"button","type","submit","class","aw-quiet"); fputs("Remove this work",stdout); ui_close(stdout,"button"); ui_close(stdout,"form"); ui_close(stdout,"details");
        wl_free(&t);
    }
    if(n<AWL_WORK_LIMIT){fputs("<details data-lit-new",stdout);if(!n)fputs(" open",stdout);fputs("><summary>Add a work</summary>",stdout);
        wl_form(site_id,world,space,"work",revision,"new-work");
        ui_open(stdout,"label",NULL); fputs("Short name",stdout); ui_open(stdout,"small",NULL); fputs("Lowercase, used in the link",stdout); ui_close(stdout,"small"); ui_open_a(stdout,"input","name","work","required",(const char *)NULL,"maxlength","64","pattern","[a-z0-9][a-z0-9-]*"); ui_close(stdout,"label");
        wl_work_fields(NULL,works,n,&site);ui_open(stdout,"div","aw-actions"); ui_open_a(stdout,"button","type","submit"); fputs("Add the work",stdout); ui_close(stdout,"button"); ui_close(stdout,"div"); ui_close(stdout,"form"); ui_close(stdout,"details");}
    /* reading circle: the space's club, nothing separate */
    char club[8]="";aw_club_state(db,subject,site_id,world,space,club);
    ui_text(stdout,"h3",NULL,"Reading circle"); ui_open(stdout,"p",NULL);
    fputs(!strcmp(club,"open")?"The circle is open. Signed-in readers join from the public page, propose what to read next and talk about it. Only members can read what is said.":
          "Open a circle and readers can join from the public page once the space is published. Members propose what to read and talk about it; you can hide posts and take people out.",stdout);
    ui_close(stdout,"p"); ui_open(stdout,"div","aw-actions"); ui_open_a(stdout,"form","method","post","action","/workspace/world/film","data-lit-form","circle");
    {char wn[24];ph_hidden("site",site_id);snprintf(wn,sizeof(wn),"%lld",(long long)world);ph_hidden("world",wn);ph_hidden("operation","club");ph_hidden("space",space);ph_hidden("return","studio");}
    ph_hidden("state",!strcmp(club,"open")?"closed":"open");fputs("<button type=\"submit\"",stdout);if(!strcmp(club,"open"))fputs(" class=\"aw-quiet\"",stdout);
    fputs(">",stdout);fputs(!strcmp(club,"open")?"Close the circle":"Open a reading circle",stdout);ui_close(stdout,"button"); ui_close(stdout,"form");
    if(!strcmp(club,"open")){fputs("<a class=\"ph-button\" href=\"/workspace/worlds/club?site=",stdout);url_query_value(site_id);fputs("&amp;space=",stdout);url_query_value(space);fputs("\">Go to the circle</a>",stdout);}
    ui_close(stdout,"div"); ui_close(stdout,"section");free(works);
}

/* Club picks and posts may name a work as "works/<key>": its options and title. */
static void wl_club_options(sqlite3 *db,const char *ns,sqlite3_int64 world,const char *space){
    WlWork *w=NULL;int n=wl_works(db,NULL,ns,world,space,&w);
    for(int i=0;i<n;i++){ui_open_a(stdout,"option",UI_DYN,"value","works/",(w[i].key));html(w[i].title);fputs(", ",stdout);html(w[i].author);ui_close(stdout,"option");}
    free(w);
}
static int wl_club_title(sqlite3 *db,const PhSite *site,sqlite3_int64 world,const char *ref,int link){
    char home[65],work[65];if(!ref||strncmp(ref,"works/",6)||awl_public_key(db,site->id,world,ref+6,home,work)!=1)return 0;
    WlText t;if(!wl_load(db,NULL,site->id,world,home,work,&t))return 0;
    if(link){fputs("<a href=\"https://",stdout);html(site->host);wl_read_href(home,work,NULL);fputs("\">",stdout);}html(t.w.title);if(link)ui_close(stdout,"a");
    wl_free(&t);return 1;
}

#endif
