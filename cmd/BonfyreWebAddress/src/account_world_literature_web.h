#ifndef BONFYRE_ACCOUNT_WORLD_LITERATURE_WEB_H
#define BONFYRE_ACCOUNT_WORLD_LITERATURE_WEB_H
#include "../../BonfyreCMS/src/account_world_literature.h"

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
            if(j<n){wl_esc(s+from,i-from);fputs("<em>",stdout);wl_esc(s+i+1,j-i-1);fputs("</em>",stdout);i=j+1;from=i;continue;}
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
        fputs("<div class=\"aw-verse\" data-world-verse>",stdout);
        for(const char *p=body;*p||p==body;){
            const char *e=strchr(p,'\n');size_t len=e?(size_t)(e-p):strlen(p);unit++;
            int blank=strspn(p," \t")>=len;
            printf("<span class=\"aw-line%s\" id=\"l-%d\" data-unit=\"%d\">",blank?" aw-gap":"",unit,unit);
            if(unit%5==0&&!blank)printf("<span class=\"aw-ln\" aria-hidden=\"true\">%d</span>",unit);
            wl_inline(p,len);if(marks)wl_markers(t,t->p[pi].passage,unit,&seq);fputs("</span>\n",stdout);
            if(!e||!e[1])break;p=e+1;
        }
        fputs("</div>",stdout);return;
    }
    fputs("<div class=\"aw-prose\" data-world-prose>",stdout);
    int open=0;
    for(const char *p=body;*p;){
        const char *e=strchr(p,'\n');size_t len=e?(size_t)(e-p):strlen(p);int blank=strspn(p," \t")>=len;
        if(blank){if(open){if(marks)wl_markers(t,t->p[pi].passage,unit,&seq);fputs("</p>",stdout);open=0;}}
        else{size_t lead=strspn(p," \t");if(!open){unit++;printf("<p id=\"p-%d\" data-unit=\"%d\">",unit,unit);open=1;}else fputc(' ',stdout);wl_inline(p+lead,len-lead);}
        if(!e)break;p=e+1;
    }
    if(open){if(marks)wl_markers(t,t->p[pi].passage,unit,&seq);fputs("</p>",stdout);}
    fputs("</div>",stdout);
}
static void wl_notes(const WlText *t,int pi){
    int seq=0,any=0;const char *passage=t->p[pi].passage;
    for(int g=0;g<t->ng;g++)if(!strcmp(t->g[g].passage,passage)){any=1;break;}
    if(!any)return;
    fputs("<section class=\"aw-notes\" aria-labelledby=\"aw-notes-h\"><h2 id=\"aw-notes-h\" class=\"region-label\">Notes</h2><ol>",stdout);
    /* the same order the markers were numbered in: unit, then kind, then id */
    for(int g=0;g<t->ng;g++){const WlGloss *x=&t->g[g];if(strcmp(x->passage,passage))continue;seq++;
        printf("<li id=\"n-%lld\" value=\"%d\" data-gloss=\"%s\"><span class=\"aw-at\">%s %d</span> ",x->id,seq,x->kind,wl_is_poem(t->w.kind)?"Line":"Paragraph",x->unit);
        html(x->body);printf(" <a href=\"#r-%lld\" aria-label=\"Back to the text\">&#8617;</a></li>",x->id);}
    fputs("</ol></section>",stdout);
}
static void wl_credit(const WlWork *w){
    fputs("<p class=\"by\">",stdout);html(w->author);if(w->translator[0]){fputs(", translated by ",stdout);html(w->translator);}fputs("</p>",stdout);
}
static void wl_terms(const WlWork *w){
    fputs("<p class=\"aw-terms\" data-world-rights=\"",stdout);html(w->rights);fputs("\">",stdout);
    if(!strcmp(w->rights,"own")){fputs("Text &copy; ",stdout);html(w->author);fputs(". ",stdout);html(w->licence);}
    else html(w->licence);
    if(w->provenance[0]){fputs(" &middot; <a rel=\"noopener\" href=\"",stdout);html(w->provenance);fputs("\">",stdout);
        html(w->provenance_label[0]?w->provenance_label:"Where this text was taken from");fputs("</a>",stdout);}
    fputs("</p>",stdout);
}
/* Recitation: plays only under open terms its own source states now. */
static void wl_recitation(sqlite3 *db,const WlWork *w){
    if(!w->recitation[0])return;
    PageMaterial *m=calloc(1,sizeof(*m));PageItem *it=calloc(1,sizeof(*it));char loaded[96]="";
    int found=m&&it&&world_web_resolve(db,w->recitation,m,loaded,it);
    if(found&&wa_open_audio(it)){fputs("<div class=\"aw-recite\" data-recitation=\"playing\"><button type=\"button\" class=\"ak-btn is-primary\" data-ak-track data-ak-default data-src=\"",stdout);html(it->asset);
        fputs("\" data-title=\"",stdout);html(w->title);fputs("\" data-by=\"",stdout);html(it->by[0]?it->by:it->title);fputs("\">Hear it read</button><p class=\"aw-terms\">",stdout);html(it->title);if(it->by[0]){fputs(", ",stdout);html(it->by);}fputs(" &middot; ",stdout);html(it->rights);fputs("</p></div>",stdout);}
    else if(found&&public_http_url(it->href)){fputs("<p class=\"aw-terms\" data-recitation=\"linked\"><a rel=\"noopener\" href=\"",stdout);html(it->href);
        fputs("\">Hear it read where the recording is published</a>; its source states no open terms, so it does not play here.</p>",stdout);}
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
        fputs("<section class=\"aw-works\" aria-labelledby=\"aw-works-h\" data-world-works><h2 id=\"aw-works-h\" class=\"region-label\">Writing</h2>",stdout);
        char group[81]="\x01";
        for(int i=0;i<n;i++){
            if(strcmp(group,w[i].collection)){if(group[0]!='\x01')fputs("</ol>",stdout);snprintf(group,sizeof(group),"%s",w[i].collection);
                fputs("<h3 class=\"aw-month\">",stdout);html(group[0]?group:"Works");fputs("</h3><ol class=\"aw-bib\">",stdout);}
            fputs("<li data-work=\"",stdout);html(w[i].work);fputs("\"><a href=\"",stdout);wl_read_href(space,w[i].work,NULL);fputs("\">",stdout);html(w[i].title);
            fputs("</a> <span class=\"aw-kind\">",stdout);html(wl_kind_word(w[i].kind));if(strcmp(w[i].language,"en")){fputs(" &middot; ",stdout);html(wl_language_word(w[i].language));}
            fputs("</span><span class=\"by\">",stdout);html(w[i].author);if(w[i].translator[0]){fputs(", translated by ",stdout);html(w[i].translator);}fputs("</span></li>",stdout);
        }
        fputs("</ol></section>",stdout);
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
    fputs("\" data-shown=\"text\"><div class=\"aw-words\"><p class=\"aw-role\">",stdout);world_web_words(role);fputs("</p><h2><a href=\"",stdout);wl_read_href(home,work,NULL);fputs("\">",stdout);html(t.w.title);fputs("</a></h2>",stdout);
    wl_credit(&t.w);if(note&&*note){fputs("<p class=\"aw-note\">",stdout);html(note);fputs("</p>",stdout);}
    wl_recitation(db,&t.w);
    if(pi>=0&&wl_is_poem(t.w.kind))wl_text(&t,pi,0);
    else if(pi>=0){ /* the opening paragraph, then the way in */
        const char *b=t.p[pi].body,*end=strstr(b,"\n\n");size_t len=end?(size_t)(end-b):strlen(b);if(len>700){len=700;while(len&&((unsigned char)b[len]&0xC0)==0x80)len--;}
        fputs("<p class=\"aw-body aw-opening\">",stdout);
        for(size_t i=0;i<len;i++){if(b[i]=='\n'){if(i+1<len)fputc(' ',stdout);}else wl_esc(b+i,1);}
        if(len==700)fputs("&hellip;",stdout);fputs("</p>",stdout);
    }
    fputs("<p class=\"aw-listen\"><a href=\"",stdout);wl_read_href(home,work,NULL);fputs("\">",stdout);
    fputs(wl_is_poem(t.w.kind)?"Open it in the reader":!strcmp(t.w.kind,"interactive")?"Begin the story":!strcmp(t.w.kind,"serial")?"Read the chapters":"Keep reading",stdout);
    fputs("</a></p>",stdout);wl_terms(&t.w);fputs("</div></article>",stdout);wl_free(&t);
}

/* ------------------------------------------------------------- reader */
static void wl_reader_bar(const PhSite *site,const char *space,const char *space_title,const char *work,const char *work_title){
    fputs("<nav class=\"aw-crumb\" aria-label=\"Where you are\"><a class=\"home\" href=\"/\">",stdout);html(site->name);fputs("</a><span class=\"gr\"><a href=\"/worlds/",stdout);html(space);fputs("\">",stdout);
    html(space_title);fputs("</a>",stdout);if(work){fputs(" &rsaquo; <a href=\"",stdout);wl_read_href(space,work,NULL);fputs("\">",stdout);html(work_title);fputs("</a>",stdout);}fputs("</span></nav>",stdout);
}
static void wl_controls(int poem){
    fputs("<div class=\"aw-readctl\" role=\"group\" aria-label=\"Reading settings\" data-reader-controls>"
          "<button type=\"button\" data-size=\"-1\" aria-label=\"Smaller text\">A&minus;</button><button type=\"button\" data-size=\"1\" aria-label=\"Larger text\">A+</button>",stdout);
    if(poem)fputs("<button type=\"button\" data-numbers aria-pressed=\"false\">Line numbers</button>",stdout);
    fputs("</div>",stdout);
}
static void wl_place_link(const PhSite *site,const char *space,const char *work,const char *passage,const char *path,int poem){
    fputs("<p class=\"aw-place\"><a data-place data-unit-kind=\"",stdout);fputs(poem?"l":"p",stdout);fputs("\" href=\"https://aurekai.",stdout);html(site->territory);
    fputs("/workspace/worlds/reading?site=",stdout);url_query_value(site->id);fputs("&amp;space=",stdout);url_query_value(space);fputs("&amp;work=",stdout);url_query_value(work);
    fputs("&amp;passage=",stdout);url_query_value(passage);if(path&&*path){fputs("&amp;path=",stdout);url_query_value(path);}fputs("&amp;unit=0\">Keep my place in your account</a></p>",stdout);
}
static void wl_script(void){
    fputs("<script>(()=>{const b=document.body,k='aw-read-size',get=n=>{try{return localStorage.getItem(n)}catch(e){return null}},put=(n,v)=>{try{localStorage.setItem(n,v)}catch(e){}};"
          "let size=+(get(k)||0);const apply=()=>{b.style.setProperty('--aw-read-step',size)};apply();"
          "document.querySelectorAll('[data-size]').forEach(x=>x.addEventListener('click',()=>{size=Math.max(-2,Math.min(4,size+ +x.dataset.size));put(k,size);apply()}));"
          "const num=document.querySelector('[data-numbers]');if(num){const on=get('aw-read-numbers')==='1';b.toggleAttribute('data-numbered',on);num.setAttribute('aria-pressed',on);"
          "num.addEventListener('click',()=>{const v=!b.hasAttribute('data-numbered');b.toggleAttribute('data-numbered',v);num.setAttribute('aria-pressed',v);put('aw-read-numbers',v?'1':'0')})}"
          "const keep=document.querySelector('[data-place]'),units=[...document.querySelectorAll('[data-unit]')],work=document.querySelector('[data-world-reader]');"
          "if(work){const store='aw-place:'+location.host+work.dataset.worldReader;let unit=0;const save=()=>{if(keep){const u=new URL(keep.href);u.searchParams.set('unit',unit);keep.href=u}"
          "put(store,JSON.stringify({href:location.pathname+location.search+(unit?'#'+(keep?keep.dataset.unitKind:'p')+'-'+unit:''),unit}))};"
          "if(units.length&&'IntersectionObserver' in window){const io=new IntersectionObserver(es=>{for(const e of es)if(e.isIntersecting){unit=+e.target.dataset.unit;save();break}},{rootMargin:'0px 0px -70% 0px'});units.forEach(u=>io.observe(u))}"
          "if(work.dataset.readerPage==='contents'){const back=document.querySelector('[data-continue]');let saved=null;try{saved=JSON.parse(get(store)||'null')}catch(e){}"
          "if(back&&saved&&saved.href){back.href=saved.href;back.hidden=false}}else save()}})()</script>",stdout);
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
    fputs("<header class=\"hero aw-readhead\"><div class=\"aw-readhead-main\"><p class=\"aw-role\">",stdout);html(wl_kind_word(t.w.kind));if(t.w.collection[0]){fputs(" &middot; ",stdout);html(t.w.collection);}
    fputs("</p><h1>",stdout);html(t.w.title);fputs("</h1>",stdout);wl_credit(&t.w);
    if(pi>=0&&t.np>1&&t.p[pi].title[0]){fputs("<h2 class=\"aw-chapter\">",stdout);if(!interactive)printf("<span>%d</span> ",t.p[pi].number);html(t.p[pi].title);fputs("</h2>",stdout);}
    fputs("</div><div class=\"aw-readhead-acts\">",stdout);
    if(pi>=0){wl_recitation(db,&t.w);fputs("<button type=\"button\" class=\"ak-btn\" data-ak-track data-narrate=\"#aw-text\" data-title=\"",stdout);html(t.w.title);fputs("\" data-by=\"",stdout);html(t.w.author);fputs("\">Read it aloud</button>",stdout);}
    wl_controls(poem);
    fputs("</div><dl class=\"aw-facts\"><div><dt>Form</dt><dd>",stdout);html(wl_kind_word(t.w.kind));fputs("</dd></div><div><dt>Language</dt><dd>",stdout);html(wl_language_word(t.w.language));
    printf("</dd></div><div><dt>%s</dt><dd>%d</dd></div><div><dt>Terms</dt><dd>",poem?"Parts":"Passages",t.np);
    if(!strcmp(t.w.rights,"own")){fputs("&copy; ",stdout);html(t.w.author);}else html(t.w.licence[0]?t.w.licence:t.w.rights);
    fputs("</dd></div></dl></header>",stdout);
    if(pi<0){ /* contents */
        fputs("<nav class=\"aw-contents\" aria-label=\"Contents\"><p><a class=\"aw-door\" data-continue hidden href=\"#\">Continue where you left off</a></p><ol>",stdout);
        int first_open=-1,next_wait=-1;
        for(int i=0;i<t.np;i++){int o=wl_open(&t.p[i],today);if(o&&first_open<0)first_open=i;
            printf("<li data-passage=\"%s\" data-open=\"%d\"><span class=\"aw-at\">%d</span> ",t.p[i].passage,o,t.p[i].number);
            if(o){fputs("<a href=\"",stdout);wl_read_href(space,work,t.p[i].passage);fputs("\">",stdout);html(t.p[i].title[0]?t.p[i].title:t.w.title);fputs("</a>",stdout);
                if(t.p[i].release_on[0]){fputs(" <time class=\"aw-when\" datetime=\"",stdout);html(t.p[i].release_on);fputs("\">",stdout);cine_when(t.p[i].release_on);fputs("</time>",stdout);}}
            else{if(next_wait<0)next_wait=i;html(t.p[i].title[0]?t.p[i].title:"A chapter to come");fputs(" <span class=\"aw-when\">Arrives <time datetime=\"",stdout);html(t.p[i].release_on);fputs("\">",stdout);cine_when(t.p[i].release_on);fputs("</time></span>",stdout);}
            fputs("</li>",stdout);}
        fputs("</ol>",stdout);
        if(first_open>=0){fputs("<p><a class=\"aw-door\" href=\"",stdout);wl_read_href(space,work,t.p[first_open].passage);fputs("\">Start reading</a></p>",stdout);}
        else fputs("<p class=\"aw-note\">Nothing is out yet.</p>",stdout);
        fputs("</nav>",stdout);
    }else{
        fputs("<article class=\"aw-text\" id=\"aw-text\" data-passage=\"",stdout);html(t.p[pi].passage);fputs("\">",stdout);wl_text(&t,pi,1);fputs("</article>",stdout);
        if(interactive){
            int choices=0;for(int c=0;c<t.nc;c++)if(!strcmp(t.c[c].passage,t.p[pi].passage)){int to=wl_find(&t,t.c[c].target);if(to<0)continue;
                if(!choices++)fputs("<nav class=\"aw-choices\" aria-label=\"What happens next\" data-world-choices><ol>",stdout);
                fputs("<li><a href=\"",stdout);wl_read_href(space,work,t.c[c].target);fputs("?path=",stdout);
                char next[sizeof(path)+70];snprintf(next,sizeof(next),"%s.%s",path,t.c[c].target);url_query_value(next);fputs("\">",stdout);html(t.c[c].label);fputs("</a></li>",stdout);}
            if(choices)fputs("</ol></nav>",stdout);
            else{fputs("<p class=\"aw-ending\" data-world-ending>This is one of the endings. <a href=\"",stdout);wl_read_href(space,work,NULL);fputs("\">Begin again</a></p>",stdout);}
            fputs("<p class=\"aw-terms aw-path\" data-world-path=\"",stdout);html(path);fputs("\">Your way here: ",stdout);
            const char *p=path;int first=1;char step[65];
            while(*p){const char *dot=strchr(p,'.');size_t len=dot?(size_t)(dot-p):strlen(p);if(len>64)break;memcpy(step,p,len);step[len]=0;int at=wl_find(&t,step);
                if(!first)fputs(" &rarr; ",stdout);first=0;html(at>=0&&t.p[at].title[0]?t.p[at].title:step);if(!dot)break;p=dot+1;}
            fputs("</p>",stdout);
        }else if(t.np>1){
            int prev=-1,next=-1,wait=-1;for(int i=pi-1;i>=0&&prev<0;i--)if(wl_open(&t.p[i],today))prev=i;
            for(int i=pi+1;i<t.np&&next<0&&wait<0;i++){if(wl_open(&t.p[i],today))next=i;else wait=i;}
            fputs("<nav class=\"aw-turn\" aria-label=\"Chapters\">",stdout);
            if(prev>=0){fputs("<a rel=\"prev\" href=\"",stdout);wl_read_href(space,work,t.p[prev].passage);fputs("\">&larr; ",stdout);html(t.p[prev].title[0]?t.p[prev].title:"Before");fputs("</a>",stdout);}
            fputs("<a href=\"",stdout);wl_read_href(space,work,NULL);fputs("\">Contents</a>",stdout);
            if(next>=0){fputs("<a rel=\"next\" href=\"",stdout);wl_read_href(space,work,t.p[next].passage);fputs("\">",stdout);html(t.p[next].title[0]?t.p[next].title:"Next");fputs(" &rarr;</a>",stdout);}
            else if(wait>=0){fputs("<span class=\"aw-when\" data-next-release>The next chapter arrives <time datetime=\"",stdout);html(t.p[wait].release_on);fputs("\">",stdout);cine_when(t.p[wait].release_on);fputs("</time></span>",stdout);}
            fputs("</nav>",stdout);
        }
        wl_notes(&t,pi);
        wl_place_link(&site,space,work,t.p[pi].passage,path,poem);
    }
    /* other editions of the same text in this space */
    WlWork *all=NULL;int n=wl_works(db,NULL,site.id,world,space,&all),shown=0;
    for(int i=0;i<n;i++){WlWork *o=&all[i];if(!strcmp(o->work,t.w.work))continue;
        int related=!strcmp(o->original,t.w.work)||!strcmp(t.w.original,o->work)||(t.w.original[0]&&!strcmp(o->original,t.w.original));if(!related)continue;
        if(!shown++)fputs("<nav class=\"aw-editions\" aria-label=\"Other editions\"><h2 class=\"region-label\">Other editions</h2><ul>",stdout);
        fputs("<li><a href=\"",stdout);wl_read_href(space,o->work,NULL);fputs("\" hreflang=\"",stdout);html(o->language);fputs("\">",stdout);html(o->title);fputs("</a> <span>",stdout);html(wl_language_word(o->language));
        if(!strcmp(t.w.original,o->work))fputs(", the original",stdout);else if(o->translator[0]){fputs(", translated by ",stdout);html(o->translator);}fputs("</span></li>",stdout);}
    if(shown)fputs("</ul></nav>",stdout);free(all);
    fputs("<div class=\"src\">",stdout);wl_terms(&t.w);fputs("</div>",stdout);wl_script();world_room_close();
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
    int reader=!strcmp(op,"place")||!strcmp(op,"unplace")||!strcmp(op,"places");
    if((!reader&&!world_web_number(val[WL_WORLD],&world,0))||(post&&!reader&&!world_web_number(val[WL_REVISION],&rev,0))||(get&&strcmp(op,"draft")&&strcmp(op,"places"))||
       (post&&!strcmp(op,"places"))||(reader&&!strcmp(val[WL_RETURN],"studio"))||(!reader&&!strcmp(val[WL_RETURN],"reading"))){free(buf);aw_error(400,"Invalid workspace request.\n");return 1;}
    /* text arrives with the browser's CRLF line ends; the owner keeps "\n" */
    {char *s=val[WL_BODY],*d=s;for(;*s;s++)if(*s!='\r')*d++=*s;*d=0;}
    const char *path=getenv("BONFYRE_CMS_DB");sqlite3 *db=NULL;char subject[160],again[160];const char *site=val[WL_SITE],*space=val[WL_SPACE];
    if(!path||!*path||sqlite3_open_v2(path,&db,post?SQLITE_OPEN_READWRITE:SQLITE_OPEN_READONLY,NULL)!=SQLITE_OK){if(db)sqlite3_close(db);free(buf);aw_error(503,"Workspace unavailable.\n");return 1;}
    sqlite3_busy_timeout(db,2000);
    #define WL_ADMIT(out) (reader?cine_member_admit(bearer,auth_db,db,site,post,out):world_web_admit(bearer,auth_db,db,site,post,out))
    if(!WL_ADMIT(subject)){sqlite3_close(db);free(buf);aw_error(404,"Workspace unavailable.\n");return 1;}
    if(post&&awl_initialize(db)!=1){sqlite3_close(db);free(buf);aw_error(503,"Workspace unavailable.\n");return 1;}
    if(post&&(!WL_ADMIT(again)||strcmp(subject,again))){sqlite3_close(db);free(buf);aw_error(404,"Workspace unavailable.\n");return 1;}
    #undef WL_ADMIT
    int r=0,R=(int)rev;long long a=0,b=0;
    if(get){
        bf_jsonw w;bf_jsonw_init(&w,0);bf_jsonw_obj(&w);
        if(!strcmp(op,"places")){
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
    if(!strcmp(op,"work")){if(wl_int(val[WL_POSITION],1,999,&a))r=awl_work_put(db,subject,site,world,space,val[WL_WORK],val[WL_KIND],val[WL_TITLE],val[WL_AUTHOR],val[WL_TRANSLATOR],
                              val[WL_LANGUAGE],val[WL_ORIGINAL],val[WL_COLLECTION],val[WL_RIGHTS],val[WL_LICENCE],val[WL_PROVENANCE],val[WL_SOURCE_LABEL],val[WL_RECITATION],(int)a,R);}
    else if(!strcmp(op,"unwork"))r=awl_work_remove(db,subject,site,world,space,val[WL_WORK],R);
    else if(!strcmp(op,"passage")){if(wl_int(val[WL_NUMBER],1,9999,&a)&&(!*val[WL_START]||!strcmp(val[WL_START],"1")))
        r=awl_passage_put(db,subject,site,world,space,val[WL_WORK],val[WL_PASSAGE],(int)a,val[WL_TITLE],val[WL_BODY],val[WL_RELEASE_ON],*val[WL_START]=='1',R);}
    else if(!strcmp(op,"unpassage"))r=awl_passage_remove(db,subject,site,world,space,val[WL_WORK],val[WL_PASSAGE],R);
    else if(!strcmp(op,"choice")){if(wl_int(val[WL_NUMBER],1,AWL_CHOICE_LIMIT,&a))r=awl_choice_put(db,subject,site,world,space,val[WL_WORK],val[WL_PASSAGE],(int)a,val[WL_LABEL],val[WL_TARGET],R);}
    else if(!strcmp(op,"unchoice")){if(wl_int(val[WL_NUMBER],1,AWL_CHOICE_LIMIT,&a))r=awl_choice_remove(db,subject,site,world,space,val[WL_WORK],val[WL_PASSAGE],(int)a,R);}
    else if(!strcmp(op,"gloss")){if(wl_int(val[WL_UNIT],1,100000,&a))r=awl_gloss_put(db,subject,site,world,space,val[WL_WORK],val[WL_PASSAGE],(int)a,val[WL_NOTE_KIND],val[WL_LABEL],R);}
    else if(!strcmp(op,"ungloss")){if(wl_int(val[WL_ID],1,INT64_MAX/2,&b))r=awl_gloss_remove(db,subject,site,world,space,b,R);}
    else if(!strcmp(op,"place")){if(wl_int(val[WL_UNIT],0,100000,&a))r=awl_place_put(db,subject,site,space,val[WL_WORK],val[WL_PASSAGE],(int)a,val[WL_PATH]);}
    else if(!strcmp(op,"unplace"))r=awl_place_clear(db,subject,site,space,val[WL_WORK]);
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
    fputs("<main class=\"wrap aw-room\" data-world-reading=\"",stdout);html(space);fputs("\">",stdout);
    fputs("<nav class=\"aw-crumb\" aria-label=\"Where you are\"><a class=\"home\" href=\"https://",stdout);html(site.host);fputs("/worlds/",stdout);html(space);fputs("\">",stdout);html(title);
    fputs("</a><span class=\"ch\">your reading</span></nav><header class=\"hero\"><h1>Your place in ",stdout);html(title);
    fputs("</h1><p class=\"lede\">Where you stopped in each piece, kept with your account so you can pick up on any device. Only you can see this page.</p></header>",stdout);
    WlWork *works=NULL;int nw=wl_works(db,NULL,site_id,world,space,&works);
    if(*work){ /* confirm before keeping */
        WlText t;
        if(wl_load(db,NULL,site_id,world,space,work,&t)){int pi=wl_find(&t,passage);
            fputs("<section class=\"aw-join\" data-reading-keep><h2 class=\"region-label\">Keep this place</h2><p class=\"aw-note\">",stdout);html(t.w.title);
            if(pi>=0&&t.np>1&&t.p[pi].title[0]){fputs(", ",stdout);html(t.p[pi].title);}
            if(unit>0)printf(", %s %lld",wl_is_poem(t.w.kind)?"line":"paragraph",unit);fputs("</p>",stdout);
            fputs("<form method=\"post\" action=\"/workspace/world-literature\">",stdout);ph_hidden("site",site_id);ph_hidden("space",space);ph_hidden("operation","place");
            ph_hidden("work",work);ph_hidden("passage",passage);char u[24];snprintf(u,sizeof(u),"%lld",unit);ph_hidden("unit",u);ph_hidden("path",path);ph_hidden("return","reading");
            fputs("<button type=\"submit\">Keep my place</button></form></section>",stdout);wl_free(&t);}
    }
    fputs("<section class=\"aw-picks\" aria-labelledby=\"aw-places-h\"><h2 id=\"aw-places-h\" class=\"region-label\">Where you are reading</h2><ol data-reading-places>",stdout);
    sqlite3_stmt *s=NULL;int have=0;
    if(awl_places(db,viewer,site_id,space,&s)==1)while(sqlite3_step(s)==SQLITE_ROW){
        const char *w=wl_col(s,0),*p=wl_col(s,1),*pa=wl_col(s,3);int u=sqlite3_column_int(s,2);const WlWork *x=NULL;
        for(int i=0;i<nw;i++)if(!strcmp(works[i].work,w))x=&works[i];if(!x)continue;have++;
        fputs("<li data-reading-place=\"",stdout);html(w);fputs("\"><div class=\"aw-words\"><h3><a href=\"https://",stdout);html(site.host);wl_read_href(space,w,p);
        if(*pa){fputs("?path=",stdout);url_query_value(pa);}if(u)printf("#%s-%d",wl_is_poem(x->kind)?"l":"p",u);fputs("\">",stdout);html(x->title);fputs("</a></h3><p class=\"by\">",stdout);
        if(u)printf("%s %d &middot; ",wl_is_poem(x->kind)?"Line":"Paragraph",u);fputs("kept ",stdout);cine_when(wl_col(s,4));fputs("</p></div><div class=\"aw-acts\">",stdout);
        fputs("<form method=\"post\" action=\"/workspace/world-literature\">",stdout);ph_hidden("site",site_id);ph_hidden("space",space);ph_hidden("operation","unplace");ph_hidden("work",w);ph_hidden("return","reading");
        fputs("<button type=\"submit\" class=\"aw-quiet\">Forget</button></form></div></li>",stdout);
    }
    sqlite3_finalize(s);if(!have)fputs("<li><p class=\"aw-body\">You have not kept a place here yet.</p></li>",stdout);
    fputs("</ol></section>",stdout);world_room_close();free(works);sqlite3_close(db);return 1;
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
    if(na>=MAXL||nb>=MAXL){fputs("<p class=\"aw-terms\">Too long to compare line by line here.</p>",stdout);return;}
    short *L=calloc((size_t)(na+1)*(size_t)(nb+1),sizeof(short));if(!L)return;
    #define LL(i,j) L[(size_t)(i)*(size_t)(nb+1)+(size_t)(j)]
    for(int i=na-1;i>=0;i--)for(int j=nb-1;j>=0;j--)LL(i,j)=al[i]==bl[j]&&!memcmp(a[i],b[j],al[i])?(short)(LL(i+1,j+1)+1):(LL(i+1,j)>=LL(i,j+1)?LL(i+1,j):LL(i,j+1));
    fputs("<ol class=\"aw-diff\" data-lit-diff>",stdout);int i=0,j=0;
    while(i<na||j<nb){
        if(i<na&&j<nb&&al[i]==bl[j]&&!memcmp(a[i],b[j],al[i])){fputs("<li>",stdout);wl_esc(a[i],al[i]);fputs("</li>",stdout);i++;j++;}
        else if(j<nb&&(i>=na||LL(i,j+1)>=LL(i+1,j))){fputs("<li class=\"ins\"><ins>",stdout);wl_esc(b[j],bl[j]);fputs("</ins></li>",stdout);j++;}
        else{fputs("<li class=\"del\"><del>",stdout);wl_esc(a[i],al[i]);fputs("</del></li>",stdout);i++;}
    }
    #undef LL
    fputs("</ol>",stdout);free(L);
}
static void wl_work_fields(const WlWork *w,const WlWork *all,int n,const PhSite *site){
    const char *kinds[]={"poem","story","essay","memoir","serial","interactive"};
    fputs("<div class=\"aw-row\"><label>Title<input name=\"title\" required maxlength=\"256\" value=\"",stdout);html(w?w->title:"");
    fputs("\"></label><label>What it is<select name=\"kind\">",stdout);
    for(int i=0;i<6;i++){fputs("<option value=\"",stdout);fputs(kinds[i],stdout);fputs("\"",stdout);if(w&&!strcmp(w->kind,kinds[i]))fputs(" selected",stdout);fputs(">",stdout);html(wl_kind_word(kinds[i]));fputs("</option>",stdout);}
    fputs("</select></label></div><div class=\"aw-row\"><label>Written by<input name=\"author\" required maxlength=\"160\" value=\"",stdout);html(w?w->author:site->name);
    fputs("\"></label><label>Translated by<small>Optional</small><input name=\"translator\" maxlength=\"160\" value=\"",stdout);html(w?w->translator:"");
    fputs("\"></label><label>Language<small>A code such as en or es</small><input name=\"language\" required maxlength=\"16\" pattern=\"[A-Za-z-]{2,16}\" value=\"",stdout);html(w?w->language:"en");
    fputs("\"></label></div><div class=\"aw-row\"><label>Collection<small>Optional. Works are grouped under it</small><input name=\"collection\" maxlength=\"80\" value=\"",stdout);html(w?w->collection:"");
    fputs("\"></label><label>Translation of<small>Optional</small><select name=\"original\"><option value=\"\">Not a translation</option>",stdout);
    for(int i=0;i<n;i++){if(w&&!strcmp(all[i].work,w->work))continue;fputs("<option value=\"",stdout);html(all[i].work);fputs("\"",stdout);if(w&&!strcmp(w->original,all[i].work))fputs(" selected",stdout);fputs(">",stdout);html(all[i].title);fputs("</option>",stdout);}
    char pos[8];snprintf(pos,sizeof(pos),"%d",w?w->position:n+1);
    fputs("</select></label><label>Order<input type=\"number\" name=\"position\" min=\"1\" max=\"999\" required value=\"",stdout);html(pos);fputs("\"></label></div>",stdout);
    fputs("<div class=\"aw-row\"><label>Whose text<select name=\"rights\"><option value=\"own\"",stdout);if(!w||!strcmp(w->rights,"own"))fputs(" selected",stdout);
    fputs(">My own writing</option><option value=\"public-domain\"",stdout);if(w&&!strcmp(w->rights,"public-domain"))fputs(" selected",stdout);
    fputs(">A public-domain text</option></select></label><label>Terms<small>Yours, such as All rights reserved or CC BY 4.0. For a public-domain text, begin with Public domain</small><input name=\"licence\" required maxlength=\"128\" value=\"",stdout);
    html(w?w->licence:"All rights reserved");fputs("\"></label></div><div class=\"aw-row\"><label>Taken from<small>Required for a public-domain text: the https page, such as Wikisource or Project Gutenberg</small><input name=\"provenance\" maxlength=\"512\" value=\"",stdout);
    html(w?w->provenance:"");fputs("\"></label><label>Name of that page<small>Optional</small><input name=\"source_label\" maxlength=\"160\" value=\"",stdout);html(w?w->provenance_label:"");
    fputs("\"></label><label>Recording of it read aloud<small>Optional. The address after /o/ of the recording. It plays only if its source states open terms</small><input name=\"recitation\" maxlength=\"99\" value=\"",stdout);
    html(w?w->recitation:"");fputs("\"></label></div>",stdout);
}
static void world_lit_studio(sqlite3 *db,const char *subject,const char *site_id,sqlite3_int64 world,const char *space,long long revision){
    PhSite site;ph_site(db,NULL,site_id,&site);WlWork *works=NULL;int n=wl_works(db,subject,site_id,world,space,&works);

    fputs("<section id=\"writing\" class=\"aw-lit-studio\" data-world-lit-studio><h3>Writing</h3><p>Poems, stories, essays, memoir, serials told in chapters, and stories readers steer by choosing. "
          "Text is kept exactly as you type it: line breaks, indents and blank lines between stanzas. Put _underscores_ around words to set them in italics. "
          "Readers see a work once it is on a published space; a chapter with a date stays hidden until that day.</p>",stdout);
    /* which works are already placed on this page */
    char placed[AWL_WORK_LIMIT]={0};int next_item=1;sqlite3_stmt *items=NULL;
    if(aw_items(db,subject,site_id,world,space,&items)==1)while(sqlite3_step(items)==SQLITE_ROW){int pos=sqlite3_column_int(items,0);const char *ref=wl_col(items,1);if(pos>=next_item)next_item=pos+1;
        for(int i=0;i<n;i++)if(!strncmp(ref,"works/",6)&&!strcmp(ref+6,works[i].key))placed[i]=1;}
    sqlite3_finalize(items);
    for(int i=0;i<n;i++){
        WlText t;if(!wl_load(db,subject,site_id,world,space,works[i].work,&t))continue;const WlWork *w=&t.w;int inter=!strcmp(w->kind,"interactive");
        fputs("<details data-lit-work=\"",stdout);html(w->work);fputs("\"><summary>",stdout);html(w->title);fputs(" &middot; ",stdout);html(wl_kind_word(w->kind));
        printf(" &middot; %d passage%s",t.np,t.np==1?"":"s");if(placed[i])fputs(" &middot; on this page",stdout);fputs("</summary>",stdout);
        wl_form(site_id,world,space,"work",revision,"work");ph_hidden("work",w->work);wl_work_fields(w,works,n,&site);
        fputs("<div class=\"aw-actions\"><button type=\"submit\">Save details</button></div></form>",stdout);
        fputs("<div class=\"aw-actions\">",stdout);
        if(!placed[i]&&next_item<=AW_ITEM_LIMIT){fputs("<form method=\"post\" action=\"/workspace/world\" data-lit-form=\"place\">",stdout);world_web_hidden_base(site_id,world,space,"item",revision);
            char p[8],ref[40];snprintf(p,sizeof(p),"%d",next_item);snprintf(ref,sizeof(ref),"works/%s",w->key);ph_hidden("position",p);ph_hidden("ref",ref);ph_hidden("role",wl_is_poem(w->kind)?"poem":w->kind);
            fputs("<button type=\"submit\" class=\"aw-quiet\">Show it on this page</button></form>",stdout);}
        fputs("<span class=\"aw-terms\">To show it in another of your spaces, add the piece <code>works/",stdout);html(w->key);fputs("</code> there.</span></div>",stdout);
        /* passages */
        char *reach=calloc((size_t)t.np+1,1);int *parent=calloc((size_t)t.np+1,sizeof(int));if(inter&&reach&&parent)wl_reach(&t,reach,parent);
        for(int p=0;p<t.np;p++){const WlPassage *x=&t.p[p];
            printf("<details data-lit-passage=\"%s\"><summary>%d. ",x->passage,x->number);html(x->title[0]?x->title:x->passage);
            if(x->release_on[0]){fputs(" &middot; out ",stdout);cine_when(x->release_on);}if(x->start)fputs(" &middot; first passage",stdout);
            if(inter&&reach&&!reach[p])fputs(" &middot; <span class=\"aw-warn\">readers cannot reach this yet</span>",stdout);fputs("</summary>",stdout);
            wl_form(site_id,world,space,"passage",revision,"passage");ph_hidden("work",w->work);ph_hidden("passage",x->passage);
            char num[8];snprintf(num,sizeof(num),"%d",x->number);
            fputs("<div class=\"aw-row\"><label>Number<input type=\"number\" name=\"number\" min=\"1\" max=\"9999\" required value=\"",stdout);html(num);
            fputs("\"></label><label>Title<small>Optional for a single poem</small><input name=\"title\" maxlength=\"256\" value=\"",stdout);html(x->title);
            fputs("\"></label><label>Out on<small>Optional. Hidden from readers until this day</small><input type=\"date\" name=\"release_on\" value=\"",stdout);html(x->release_on);fputs("\"></label></div>",stdout);
            fputs("<label>Text<textarea class=\"aw-text\" name=\"body\" rows=\"12\" required maxlength=\"60000\" spellcheck=\"true\">\n",stdout);html(x->body);fputs("</textarea></label>",stdout);
            if(inter){fputs("<label class=\"aw-check\"><input type=\"checkbox\" name=\"start\" value=\"1\"",stdout);if(x->start)fputs(" checked",stdout);fputs("> Readers begin here</label>",stdout);}
            fputs("<div class=\"aw-actions\"><button type=\"submit\">Save this text</button></div></form>",stdout);
            if(inter){int nc=0,used[AWL_CHOICE_LIMIT+1]={0};
                for(int c=0;c<t.nc;c++)if(!strcmp(t.c[c].passage,x->passage)){used[t.c[c].number]=1;
                    if(!nc++)fputs("<ol class=\"aw-items\" data-lit-choices>",stdout);int to=wl_find(&t,t.c[c].target);
                    fputs("<li><div>",stdout);html(t.c[c].label);fputs(" &rarr; ",stdout);html(to>=0&&t.p[to].title[0]?t.p[to].title:t.c[c].target);fputs("</div>",stdout);
                    wl_form(site_id,world,space,"unchoice",revision,NULL);ph_hidden("work",w->work);ph_hidden("passage",x->passage);char cn[4];snprintf(cn,sizeof(cn),"%d",t.c[c].number);ph_hidden("number",cn);
                    fputs("<button type=\"submit\" class=\"aw-quiet\">Remove</button></form></li>",stdout);}
                if(nc)fputs("</ol>",stdout);else fputs("<p>No choices: this passage is an ending.</p>",stdout);
                int free_n=0;for(int k=1;k<=AWL_CHOICE_LIMIT&&!free_n;k++)if(!used[k])free_n=k;
                if(free_n){wl_form(site_id,world,space,"choice",revision,"choice");ph_hidden("work",w->work);ph_hidden("passage",x->passage);char cn[4];snprintf(cn,sizeof(cn),"%d",free_n);ph_hidden("number",cn);
                    fputs("<div class=\"aw-row\"><label>A choice readers can make<input name=\"label\" required maxlength=\"200\"></label><label>Leads to<select name=\"target\">",stdout);
                    for(int q=0;q<t.np;q++){fputs("<option value=\"",stdout);html(t.p[q].passage);fputs("\">",stdout);html(t.p[q].title[0]?t.p[q].title:t.p[q].passage);fputs("</option>",stdout);}
                    fputs("</select></label></div><div class=\"aw-actions\"><button type=\"submit\">Add the choice</button></div></form>",stdout);}}
            /* glosses on this passage */
            int ng=0;for(int g=0;g<t.ng;g++)if(!strcmp(t.g[g].passage,x->passage)){if(!ng++)fputs("<ol class=\"aw-items\" data-lit-glosses>",stdout);
                printf("<li><div><strong>%s %s %d</strong><br>",!strcmp(t.g[g].kind,"footnote")?"Footnote,":"Note,",wl_is_poem(w->kind)?"line":"paragraph",t.g[g].unit);html(t.g[g].body);fputs("</div>",stdout);
                wl_form(site_id,world,space,"ungloss",revision,NULL);char id[24];snprintf(id,sizeof(id),"%lld",t.g[g].id);ph_hidden("id",id);fputs("<button type=\"submit\" class=\"aw-quiet\">Remove</button></form></li>",stdout);}
            if(ng)fputs("</ol>",stdout);
            wl_form(site_id,world,space,"gloss",revision,"gloss");ph_hidden("work",w->work);ph_hidden("passage",x->passage);
            printf("<div class=\"aw-row\"><label>%s<input type=\"number\" name=\"unit\" min=\"1\" max=\"%d\" required value=\"1\"></label>",wl_is_poem(w->kind)?"On line":"On paragraph",awl_units(w->kind,x->body));
            fputs("<label>Kind<select name=\"note_kind\"><option value=\"footnote\">Footnote</option><option value=\"note\">Note</option></select></label></div>"
                  "<label>The note<input name=\"label\" required maxlength=\"1000\"></label><div class=\"aw-actions\"><button type=\"submit\" class=\"aw-quiet\">Add the note</button></div></form>",stdout);
            /* earlier versions */
            sqlite3_stmt *v=NULL;int nv=0;
            if(awl_versions(db,subject,site_id,world,space,w->work,x->passage,&v)==1)while(sqlite3_step(v)==SQLITE_ROW){
                if(!nv++)fputs("<details data-lit-versions><summary>Earlier versions</summary>",stdout);
                printf("<p class=\"aw-state\">Before change %d, compared with the text now</p>",sqlite3_column_int(v,0));wl_diff(wl_col(v,1),x->body);}
            sqlite3_finalize(v);if(nv)fputs("</details>",stdout);
            wl_form(site_id,world,space,"unpassage",revision,NULL);ph_hidden("work",w->work);ph_hidden("passage",x->passage);fputs("<button type=\"submit\" class=\"aw-quiet\">Remove this passage</button></form></details>",stdout);
        }
        if(inter&&t.np){int endings=0,lost=0;for(int p=0;p<t.np;p++){int out=0;for(int c=0;c<t.nc;c++)if(!strcmp(t.c[c].passage,t.p[p].passage))out++;if(!out&&reach&&reach[p])endings++;if(reach&&!reach[p])lost++;}
            printf("<p data-lit-structure>%d ending%s readers can reach.",endings,endings==1?"":"s");if(lost)printf(" <span class=\"aw-warn\">%d passage%s cannot be reached from the first one.</span>",lost,lost==1?"":"s");fputs("</p>",stdout);}
        free(reach);free(parent);
        /* new passage */
        int next=1;for(int p=0;p<t.np;p++)if(t.p[p].number>=next)next=t.p[p].number+1;
        if(t.np<AWL_PASSAGE_LIMIT){wl_form(site_id,world,space,"passage",revision,"new-passage");ph_hidden("work",w->work);char num[8];snprintf(num,sizeof(num),"%d",next);
            fputs("<h4>",stdout);fputs(!strcmp(w->kind,"serial")?"Next chapter":inter?"New passage":t.np?"Another part":"The text",stdout);fputs("</h4><div class=\"aw-row\"><label>Short name<small>Lowercase, used in the link</small><input name=\"passage\" required maxlength=\"64\" pattern=\"[a-z0-9][a-z0-9-]*\" value=\"",stdout);
            if(!t.np)fputs("text",stdout);fputs("\"></label><label>Number<input type=\"number\" name=\"number\" min=\"1\" max=\"9999\" required value=\"",stdout);html(num);
            fputs("\"></label><label>Title<small>Optional</small><input name=\"title\" maxlength=\"256\"></label><label>Out on<small>Optional</small><input type=\"date\" name=\"release_on\"></label></div>"
                  "<label>Text<textarea class=\"aw-text\" name=\"body\" rows=\"10\" required maxlength=\"60000\"></textarea></label>",stdout);
            if(inter&&!t.np)fputs("<input type=\"hidden\" name=\"start\" value=\"1\">",stdout);
            fputs("<div class=\"aw-actions\"><button type=\"submit\">Add</button></div></form>",stdout);}
        wl_form(site_id,world,space,"unwork",revision,NULL);ph_hidden("work",w->work);fputs("<button type=\"submit\" class=\"aw-quiet\">Remove this work</button></form></details>",stdout);
        wl_free(&t);
    }
    if(n<AWL_WORK_LIMIT){fputs("<details data-lit-new",stdout);if(!n)fputs(" open",stdout);fputs("><summary>Add a work</summary>",stdout);
        wl_form(site_id,world,space,"work",revision,"new-work");
        fputs("<label>Short name<small>Lowercase, used in the link</small><input name=\"work\" required maxlength=\"64\" pattern=\"[a-z0-9][a-z0-9-]*\"></label>",stdout);
        wl_work_fields(NULL,works,n,&site);fputs("<div class=\"aw-actions\"><button type=\"submit\">Add the work</button></div></form></details>",stdout);}
    /* reading circle: the space's club, nothing separate */
    char club[8]="";aw_club_state(db,subject,site_id,world,space,club);
    fputs("<h3>Reading circle</h3><p>",stdout);
    fputs(!strcmp(club,"open")?"The circle is open. Signed-in readers join from the public page, propose what to read next and talk about it. Only members can read what is said.":
          "Open a circle and readers can join from the public page once the space is published. Members propose what to read and talk about it; you can hide posts and take people out.",stdout);
    fputs("</p><div class=\"aw-actions\"><form method=\"post\" action=\"/workspace/world/film\" data-lit-form=\"circle\">",stdout);
    {char wn[24];ph_hidden("site",site_id);snprintf(wn,sizeof(wn),"%lld",(long long)world);ph_hidden("world",wn);ph_hidden("operation","club");ph_hidden("space",space);ph_hidden("return","studio");}
    ph_hidden("state",!strcmp(club,"open")?"closed":"open");fputs("<button type=\"submit\"",stdout);if(!strcmp(club,"open"))fputs(" class=\"aw-quiet\"",stdout);
    fputs(">",stdout);fputs(!strcmp(club,"open")?"Close the circle":"Open a reading circle",stdout);fputs("</button></form>",stdout);
    if(!strcmp(club,"open")){fputs("<a class=\"ph-button\" href=\"/workspace/worlds/club?site=",stdout);url_query_value(site_id);fputs("&amp;space=",stdout);url_query_value(space);fputs("\">Go to the circle</a>",stdout);}
    fputs("</div></section>",stdout);free(works);
}

/* Club picks and posts may name a work as "works/<key>": its options and title. */
static void wl_club_options(sqlite3 *db,const char *ns,sqlite3_int64 world,const char *space){
    WlWork *w=NULL;int n=wl_works(db,NULL,ns,world,space,&w);
    for(int i=0;i<n;i++){fputs("<option value=\"works/",stdout);html(w[i].key);fputs("\">",stdout);html(w[i].title);fputs(", ",stdout);html(w[i].author);fputs("</option>",stdout);}
    free(w);
}
static int wl_club_title(sqlite3 *db,const PhSite *site,sqlite3_int64 world,const char *ref,int link){
    char home[65],work[65];if(!ref||strncmp(ref,"works/",6)||awl_public_key(db,site->id,world,ref+6,home,work)!=1)return 0;
    WlText t;if(!wl_load(db,NULL,site->id,world,home,work,&t))return 0;
    if(link){fputs("<a href=\"https://",stdout);html(site->host);wl_read_href(home,work,NULL);fputs("\">",stdout);}html(t.w.title);if(link)fputs("</a>",stdout);
    wl_free(&t);return 1;
}

static int world_lit_route(const char *method,const char *target,const char *query,const char *body,const char *raw,const char *host,const char *bearer,const char *auth_db){
    if(!strcmp(target,"/workspace/world-literature"))return world_lit_api(method,query,body,raw,host,bearer,auth_db);
    if(!strcmp(target,"/workspace/worlds/reading"))return serve_world_reading(method,query,bearer,auth_db);
    if(!strncmp(target,"/worlds/",8)&&strstr(target+8,"/read/"))return serve_world_lit_public(method,target,query,host);
    return 0;
}
#endif
