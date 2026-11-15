#ifndef BONFYRE_PROCUREMENT_FLOW_H
#define BONFYRE_PROCUREMENT_FLOW_H
/* Buying for a saved room, as one guided flow on the one shell: what you need
 * (a saved purchase list) -> compare quotes in one table -> pick one -> send it
 * for approval after an explicit confirm. Every record is derived by the
 * native procurement kernel (bonfyre-procurement) and kept through the
 * workspace owner (bonfyre-cms pack-workspace) with compare-and-swap. Nothing
 * here contacts a supplier, accepts a quote, orders or pays; the approval
 * request stays pending until its authority decides. */
#include "bf_json.h"
#include "bf_jsonw.h"

#define PF_RECORD_LIMIT 512
typedef struct { char key[512]; long long rev; char *json; bf_json_doc_t *doc; const bf_json_node_t *root; } PfRec;
typedef struct { PfRec r[PF_RECORD_LIMIT]; size_t n; } PfSet;

static void pf_free(PfSet *s){for(size_t i=0;i<s->n;i++){bf_json_free(s->r[i].doc);free(s->r[i].json);}s->n=0;}
static const bf_json_node_t *pf_at(const bf_json_doc_t *doc,const bf_json_node_t *n,const char *path){
    char part[64];while(n&&path&&*path){size_t k=strcspn(path,".");if(k>=sizeof(part))return NULL;memcpy(part,path,k);part[k]=0;
        if(n->type!=BF_JSON_OBJECT)return NULL;n=bf_json_obj_get(doc,n,part);path+=k;if(*path=='.')path++;}
    return n;
}
static int pf_str(const PfRec *r,const char *path,char *out,size_t cap){
    const bf_json_node_t *n=r?pf_at(r->doc,r->root,path):NULL;out[0]=0;if(!n||n->type!=BF_JSON_STRING)return 0;
    size_t len=0;char *s=bf_json_unescape(n->val.str.ptr,n->val.str.len,&len);if(!s)return 0;
    if(len>=cap){free(s);return 0;}memcpy(out,s,len);out[len]=0;free(s);return 1;
}
static int pf_nstr(const bf_json_doc_t *doc,const bf_json_node_t *obj,const char *path,char *out,size_t cap){
    PfRec r;memset(&r,0,sizeof(r));r.doc=(bf_json_doc_t *)doc;r.root=obj;return pf_str(&r,path,out,cap);
}
static int pf_int(const bf_json_doc_t *doc,const bf_json_node_t *obj,const char *path,long long *v){
    const bf_json_node_t *n=pf_at(doc,obj,path);if(!n||n->type!=BF_JSON_INT)return 0;*v=n->val.i;return 1;
}
static void pf_money(long long cents){
    char digits[32];snprintf(digits,sizeof(digits),"%lld",cents/100);size_t len=strlen(digits);
    fputc('$',stdout);for(size_t i=0;i<len;i++){fputc(digits[i],stdout);if((len-i-1)%3==0&&i+1<len)fputc(',',stdout);}printf(".%02lld",cents%100);
}
static void pf_day(const char *utc){
    static const char *months[]={"Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"};int y,m,d;
    if(utc&&sscanf(utc,"%4d-%2d-%2d",&y,&m,&d)==3&&m>=1&&m<=12)printf("%d %s %d",d,months[m-1],y);else html(utc?utc:"");
}
static void pf_now(char out[21]){time_t t=time(NULL);struct tm tm;gmtime_r(&t,&tm);strftime(out,21,"%Y-%m-%dT%H:%M:%SZ",&tm);}
static void pf_id(const char *prefix,char *out,size_t cap){
    unsigned char b[16];arc4random_buf(b,sizeof(b));size_t n=(size_t)snprintf(out,cap,"%s",prefix);
    for(size_t i=0;i<sizeof(b)&&n+2<cap;i++)n+=(size_t)snprintf(out+n,cap-n,"%02x",b[i]);
}

/* One page of the workspace owner's records under a prefix, then the next. */
static int pf_owner(const char *operation,const char *binding,const char *subject,const char *scope,const char *actor,const char *prefix,const char *cursor,const void *input,size_t bytes,unsigned char **answer,size_t *length){
    const char *bin=page_cms_env("BONFYRE_CMS_BINARY","/usr/local/bin/bonfyre-cms"),*db=page_cms_env("BONFYRE_CMS_DB","/var/lib/bonfyre/cms/bonfyre_cms.db");
    char *const read[]={(char*)bin,"pack-workspace","--operation",(char*)operation,"--binding",(char*)binding,"--subject",(char*)subject,"--scope",(char*)scope,"--actor",(char*)actor,"--prefix",(char*)prefix,"--cursor",(char*)cursor,"--epoch","-1","--db",(char*)db,NULL};
    char *const write[]={(char*)bin,"pack-workspace","--operation",(char*)operation,"--binding",(char*)binding,"--subject",(char*)subject,"--scope",(char*)scope,"--actor",(char*)actor,"--prefix",(char*)prefix,"--db",(char*)db,NULL};
    return aw_native(input?write:read,input,bytes,answer,length);
}
static int pf_load(const char *binding,const char *subject,const char *scope,const char *actor,PfSet *set){
    char cursor[512]="";set->n=0;
    for(int pages=0;pages<16;pages++){
        unsigned char *answer=NULL;size_t length=0;AcPage *page=malloc(sizeof(AcPage));if(!page)return 0;
        if(pf_owner("read",binding,subject,scope,actor,"procurement-",cursor,NULL,0,&answer,&length)||!ac_page(answer,length,page)){free(page);free(answer);return 0;}
        for(size_t i=0;i<page->count&&set->n<PF_RECORD_LIMIT;i++){
            AcRow *row=&page->rows[i];char *json=NULL;size_t bytes=0;char err[128];
            if(!row->body||!pde_workspace_json(row->body,row->bytes,&json,&bytes))continue;
            bf_json_doc_t *doc=bf_json_parse(json,bytes,err,sizeof(err));if(!doc||!bf_json_root(doc)||bf_json_root(doc)->type!=BF_JSON_OBJECT){bf_json_free(doc);free(json);continue;}
            PfRec *r=&set->r[set->n++];snprintf(r->key,sizeof(r->key),"%s",row->key);r->rev=row->revision;r->json=json;r->doc=doc;r->root=bf_json_root(doc);
        }
        int more=page->more;snprintf(cursor,sizeof(cursor),"%s",page->cursor);free(page);free(answer);if(!more)return 1;
    }
    return 1;
}
static const PfRec *pf_find(const PfSet *s,const char *key){for(size_t i=0;i<s->n;i++)if(!strcmp(s->r[i].key,key))return &s->r[i];return NULL;}
static int pf_is(const PfRec *r,const char *prefix,const char *schema){char got[96];return !strncmp(r->key,prefix,strlen(prefix))&&pf_str(r,"schema",got,sizeof(got))&&!strcmp(got,schema);}
static int pf_eq(const PfRec *r,const char *path,const char *value){char got[512];return value&&pf_str(r,path,got,sizeof(got))&&!strcmp(got,value);}
static const PfRec *pf_latest(const PfSet *s,const char *prefix,const char *schema,const char *path,const char *value,const char *stamp){
    const PfRec *best=NULL;char a[32],b[32];
    for(size_t i=0;i<s->n;i++){const PfRec *r=&s->r[i];if(!pf_is(r,prefix,schema)||(path&&!pf_eq(r,path,value)))continue;
        if(!best||(pf_str(r,stamp,a,sizeof(a))&&pf_str(best,stamp,b,sizeof(b))&&strcmp(a,b)>0))best=r;}
    return best;
}
/* The request for quotes on a list, its quotes and the approval sent from it. */
typedef struct { const PfRec *list,*rfq,*approval,*quotes[32]; size_t nquotes; } PfFlow;
static void pf_flow(const PfSet *s,const char *list_key,PfFlow *f){
    memset(f,0,sizeof(*f));char id[160];
    if(list_key&&*list_key){f->list=pf_find(s,list_key);if(f->list&&!pf_is(f->list,"procurement-draft:","service-site.room-purchase-list.v1"))f->list=NULL;}
    if(!f->list)f->list=pf_latest(s,"procurement-draft:","service-site.room-purchase-list.v1",NULL,NULL,"created_at");
    if(!f->list||!pf_str(f->list,"id",id,sizeof(id)))return;
    f->rfq=pf_latest(s,"procurement-rfq:","bonfyre.procurement.supplier-request.v1","source.purchase_list_id",id,"requested_at");
    if(!f->rfq||!pf_str(f->rfq,"id",id,sizeof(id)))return;char digest[65];pf_str(f->rfq,"digest",digest,sizeof(digest));
    for(size_t i=0;i<s->n&&f->nquotes<32;i++){const PfRec *r=&s->r[i];if(pf_is(r,"procurement-quote:","bonfyre.procurement.supplier-quote-observation.v1")&&pf_eq(r,"source.rfq_id",id)&&pf_eq(r,"source.rfq_digest",digest))f->quotes[f->nquotes++]=r;}
    for(size_t i=0;i<s->n;i++){const PfRec *r=&s->r[i];char alloc[160],key[320];
        if(!pf_is(r,"procurement-approval:","bonfyre.procurement.approval-request.v1")||!pf_str(r,"source.allocation_id",alloc,sizeof(alloc)))continue;
        snprintf(key,sizeof(key),"procurement-allocation:%s",alloc);const PfRec *a=pf_find(s,key);
        if(a&&pf_eq(a,"source.rfq_id",id)){char x[32],y[32];if(!f->approval||(pf_str(r,"requested_at",x,sizeof(x))&&pf_str(f->approval,"requested_at",y,sizeof(y))&&strcmp(x,y)>0))f->approval=r;}
    }
}
/* A quote's price for one requested line, and its known total. */
static int pf_quote_line(const PfRec *q,const char *line_id,long long *unit,long long *extended){
    const bf_json_node_t *lines=pf_at(q->doc,q->root,"lines");if(!lines||lines->type!=BF_JSON_ARRAY)return 0;
    for(const bf_json_node_t *l=bf_json_child_first(q->doc,lines);l;l=bf_json_child_next(q->doc,l)){char rid[65];
        if(!pf_nstr(q->doc,l,"rfq_line_id",rid,sizeof(rid))||strcmp(rid,line_id))continue;
        *unit=-1;*extended=-1;pf_int(q->doc,l,"unit_cents",unit);pf_int(q->doc,l,"known_extended_cents",extended);return 1;}
    return 0;
}
static int pf_quote_total(const PfRec *q,long long *total){
    long long sum=0,v;const bf_json_node_t *lines=pf_at(q->doc,q->root,"lines");if(!lines||lines->type!=BF_JSON_ARRAY)return 0;
    for(const bf_json_node_t *l=bf_json_child_first(q->doc,lines);l;l=bf_json_child_next(q->doc,l)){if(!pf_int(q->doc,l,"known_extended_cents",&v))return 0;sum+=v;}
    const char *fees[]={"freight_cents","tax_cents","other_fees_cents"};for(int i=0;i<3;i++){if(!pf_int(q->doc,q->root,fees[i],&v))return 0;sum+=v;}
    *total=sum;return 1;
}
static void pf_supplier(const PfRec *q){char name[512];if(pf_str(q,"supplier.name",name,sizeof(name))||pf_str(q,"supplier.id",name,sizeof(name)))html(name);else fputs("Unnamed supplier",stdout);}

/* JSON from the kernel -> the workspace record envelope [4,value,[]]. */
static int pf_mp(bf_mp_writer_t *w,const bf_json_doc_t *doc,const bf_json_node_t *n,int depth){
    if(depth>32||!n)return 0;
    switch(n->type){
    case BF_JSON_NULL:return !bf_mp_pack_nil(w);
    case BF_JSON_BOOL:return !bf_mp_pack_bool(w,n->val.b);
    case BF_JSON_INT:return !bf_mp_pack_int(w,n->val.i);
    case BF_JSON_DOUBLE:return !bf_mp_pack_double(w,n->val.d);
    case BF_JSON_STRING:{size_t len=0;char *s=bf_json_unescape(n->val.str.ptr,n->val.str.len,&len);int ok=s&&!bf_mp_pack_str(w,s,(uint32_t)len);free(s);return ok;}
    case BF_JSON_ARRAY:if(bf_mp_pack_array(w,(uint32_t)n->child_count))return 0;
        for(const bf_json_node_t *c=bf_json_child_first(doc,n);c;c=bf_json_child_next(doc,c))if(!pf_mp(w,doc,c,depth+1))return 0;return 1;
    case BF_JSON_OBJECT:if(bf_mp_pack_map(w,(uint32_t)n->child_count))return 0;
        for(const bf_json_node_t *c=bf_json_child_first(doc,n);c;c=bf_json_child_next(doc,c)){size_t len=0;char *k=bf_json_unescape(c->key,c->key_len,&len);int ok=k&&!bf_mp_pack_str(w,k,(uint32_t)len);free(k);if(!ok||!pf_mp(w,doc,c,depth+1))return 0;}return 1;
    }
    return 0;
}
/* The owner's write receipt: the same rows as a read, in write order. */
static int pf_receipt(const uint8_t *body,size_t bytes,AcPage *out){
    memset(out,0,sizeof(*out));bf_mp_reader_t r;bf_mp_reader_init(&r,body,bytes);bf_mp_value_t v;long long version;
    if(!body||!ac_array(&r,5)||!ac_uint(&r,&version)||version!=1||!ac_uint(&r,&out->epoch)||bf_mp_read_next(&r,&v)!=BF_MP_BOOL)return 0;
    out->more=v.v.b;if(!ac_text(&r,out->cursor,sizeof(out->cursor))||bf_mp_read_next(&r,&v)!=BF_MP_ARRAY||v.v.count>AC_ROWS)return 0;out->count=v.v.count;
    for(size_t n=0;n<out->count;n++){AcRow *row=&out->rows[n];if(!ac_array(&r,3)||!ac_text(&r,row->key,sizeof(row->key))||!ac_uint(&r,&row->revision))return 0;
        bf_mp_type_t t=bf_mp_read_next(&r,&v);if(t!=BF_MP_BIN&&t!=BF_MP_NIL)return 0;if(t==BF_MP_BIN){row->body=v.v.raw.ptr;row->bytes=v.v.raw.len;}}
    return r.pos==r.len;
}
typedef struct { char key[320]; const char *json; } PfPut;
/* Keep new records in one atomic write; each is expected not to exist yet. */
static int pf_put(const char *binding,const char *subject,const char *scope,const char *actor,const PfPut *puts,size_t count){
    char request[65];{bf_jsonw h;bf_jsonw_init(&h,0);bf_jsonw_arr(&h);for(size_t i=0;i<count;i++){bf_jsonw_str(&h,puts[i].key);bf_jsonw_str(&h,puts[i].json?puts[i].json:"");}bf_jsonw_arr_end(&h);
        if(h.oom){bf_jsonw_free(&h);return 6;}bf_sha256_hex((const uint8_t *)h.buf,h.len,request);bf_jsonw_free(&h);}
    bf_mp_writer_t w;bf_mp_writer_init(&w,8192);
    int ok=!bf_mp_pack_array(&w,3)&&!bf_mp_pack_int(&w,1)&&!bf_mp_pack_str_cstr(&w,request)&&!bf_mp_pack_array(&w,(uint32_t)count);
    for(size_t i=0;i<count&&ok;i++){
        char err[128];bf_json_doc_t *doc=puts[i].json?bf_json_parse(puts[i].json,strlen(puts[i].json),err,sizeof(err)):NULL;bf_mp_writer_t rec;bf_mp_writer_init(&rec,4096);
        ok=doc&&!bf_mp_pack_array(&rec,3)&&!bf_mp_pack_int(&rec,4)&&pf_mp(&rec,doc,bf_json_root(doc),0)&&!bf_mp_pack_array(&rec,0);
        ok=ok&&!bf_mp_pack_array(&w,4)&&!bf_mp_pack_str_cstr(&w,puts[i].key)&&!bf_mp_pack_int(&w,0)&&!bf_mp_pack_int(&w,0)&&!bf_mp_pack_bin(&w,rec.buf,(uint32_t)rec.len);
        bf_mp_writer_free(&rec);bf_json_free(doc);
    }
    unsigned char *answer=NULL;size_t length=0;int rc=ok?pf_owner("write",binding,subject,scope,actor,puts[0].key,"",w.buf,w.len,&answer,&length):6;
    AcPage *page=ok&&!rc?malloc(sizeof(AcPage)):NULL;ok=page&&pf_receipt(answer,length,page)&&page->count==count;
    for(size_t i=0;ok&&i<count;i++){int seen=0;for(size_t k=0;k<page->count;k++)if(!strcmp(page->rows[k].key,puts[i].key)&&page->rows[k].revision==1)seen=1;ok=seen;}
    free(page);free(answer);bf_mp_writer_free(&w);return ok?0:(rc?rc:6);
}
static char *pf_kernel(const char *op,const char *input,const char *as_of){
    char bin[1024];if(!pde_binary_path(bin))return NULL;
    char *const plain[]={bin,(char*)op,NULL};char *const dated[]={bin,(char*)op,"--as-of",(char*)as_of,NULL};
    unsigned char *out=NULL;size_t bytes=0;if(aw_native(as_of?dated:plain,input,strlen(input),&out,&bytes)||!out){free(out);return NULL;}
    char *json=malloc(bytes+1);if(json){memcpy(json,out,bytes);json[bytes]=0;}free(out);return json;
}

/* ---- the page ---- */
static void pf_href(const char *binding,const char *list_key){fputs("/apps/",stdout);html(binding);fputs("/procurement/",stdout);if(list_key&&*list_key){fputs("?list=",stdout);url_query_value(list_key);}}
static void pf_stage_head(int number,const char *title,const char *state,const char *note){
    (void)number;fputs("<header class=\"ak-toolbar\"><div><h2>",stdout);html(title);fputs("</h2>",stdout);
    if(note&&*note){fputs("<p>",stdout);html(note);fputs("</p>",stdout);}
    fputs("</div><span class=\"ak-badge\">",stdout);html(state);fputs("</span></header>",stdout);
}
static const char *pf_notes[][2]={
    {"requested","Your quote request is ready. Add each quote you receive below."},
    {"quoted","Quote added. Add another, or pick one to send for approval."},
    {"sent","Sent for approval. Nothing has been ordered or paid."},
    {"prices","Enter a price for every item, and delivery, tax and fees (0 if none)."},
    {"supplier","Name the supplier this quote came from."},
    {"pick","Pick one quote to send."},
    {"confirm","Tick the box to confirm before sending."},
    {"stale","The room changed after this list was saved. Open the room and save its list again."},
    {"incomplete","That quote is missing delivery, tax or fees, or it has expired. Add a complete quote first."},
    {"changed","Something changed in another tab. Reload and try again; nothing was overwritten."},
};
static void pf_render(const char *binding,const char *subject,const char *scope,const char *actor,const char *release,const char *list_key,const char *note){
    PfSet *set=calloc(1,sizeof(PfSet));if(!set||!pf_load(binding,subject,scope,actor,set)){free(set);aw_error(503,"Your saved purchase lists could not be read. Nothing was changed.\n");return;}
    PfFlow f;pf_flow(set,list_key,&f);char lkey[512]="";if(f.list)snprintf(lkey,sizeof(lkey),"%s",f.list->key);
    int has_room=workspace_package_available(release,"product-room/");
    ak_app_http(200,1);ak_app_open("Buying","workbench","route-procurement");
    fputs("<div class=\"ak-wb-strip\"><a href=\"/apps?context=self\">Your work</a><span>/</span><b>Buying</b>",stdout);
    if(has_room){fputs("<a href=\"/apps/",stdout);html(binding);fputs("/product-room/\">Room planner</a>",stdout);}
    fputs("<span class=\"ak-live\">Nothing is ordered or paid here</span></div>",stdout);
    const char *message=NULL;for(size_t i=0;note&&i<sizeof(pf_notes)/sizeof(pf_notes[0]);i++)if(!strcmp(note,pf_notes[i][0]))message=pf_notes[i][1];
    long long list_total=0;char title[1024]="Buy what your room needs";
    if(f.list){pf_int(f.list->doc,f.list->root,"subtotal_cents",&list_total);char label[512];if(pf_str(f.list,"title",label,sizeof(label))||pf_str(f.list,"name",label,sizeof(label)))snprintf(title,sizeof(title),"%s",label);}
    fputs("<header class=\"ak-wb-hero is-solo pf-hero\"><div><p class=\"ak-kicker\">Purchase request</p><h1>",stdout);html(title);
    fputs("</h1><p class=\"ak-lede\">Start from a room's saved list, compare the quotes you get in one table, pick one, and send it for approval. Suppliers hear nothing from here.</p></div></header>",stdout);
    if(message){fputs("<div class=\"ak-pattern\"><p class=\"ak-alert\" role=\"status\">",stdout);html(message);fputs("</p></div>",stdout);}
    {   /* The flow's stages, from the records: one is current, earlier ones are done. */
        const char *steps[]={"What you need","Compare quotes","Pick one","Send for approval"};int at=f.approval?4:f.nquotes?2:f.rfq?1:0;
        fputs("<ol class=\"ak-steps\" aria-label=\"Buying steps\">",stdout);
        for(int i=0;i<4;i++){fputs(i<at?"<li class=\"is-done\">":i==at?"<li aria-current=\"step\">":"<li>",stdout);fputs(steps[i],stdout);fputs("</li>",stdout);}
        fputs("</ol>",stdout);}
    {   long long best_total=-1;size_t items=0;for(size_t i=0;i<f.nquotes;i++){long long t;if(pf_quote_total(f.quotes[i],&t)&&(best_total<0||t<best_total))best_total=t;}
        const bf_json_node_t *ls=f.list?pf_at(f.list->doc,f.list->root,"lines"):NULL;if(ls&&ls->type==BF_JSON_ARRAY)items=(size_t)ls->child_count;
        printf("<dl class=\"ak-stats is-row\"><div><dt>Items</dt><dd>%zu</dd></div><div><dt>Listed</dt><dd>",items);if(f.list)pf_money(list_total);else fputs("\xe2\x80\x94",stdout);
        printf("</dd></div><div><dt>Quotes</dt><dd>%zu</dd></div><div><dt>Lowest quote</dt><dd>",f.nquotes);if(best_total>=0)pf_money(best_total);else fputs("\xe2\x80\x94",stdout);
        fputs("</dd></div><div><dt>Status</dt><dd>",stdout);fputs(f.approval?"Waiting for approval":f.nquotes?"Ready to pick":f.rfq?"Collecting quotes":f.list?"Ready for quotes":"No list yet",stdout);fputs("</dd></div></dl>",stdout);}
    fputs("<div class=\"pf-layout\"><div class=\"pf-stages\">",stdout);

    /* 1. What you need */
    fputs("<section class=\"ak-pattern ak-card pf-stage\" id=\"need\">",stdout);
    pf_stage_head(1,"What you need",f.rfq?"Done":f.list?"Now":"Start here",f.list?"The items and amounts from your saved room.":NULL);
    if(!f.list){
        fputs("<div class=\"ak-empty-state\"><h3>Nothing to buy yet</h3><p>Plan a room and save its purchase list. It shows up here with every item and amount.</p>",stdout);
        if(has_room){fputs("<a class=\"ak-btn is-primary\" href=\"/apps/",stdout);html(binding);fputs("/product-room/\">Plan a room</a>",stdout);}
        fputs("</div>",stdout);
    }else{
        size_t lists=0;for(size_t i=0;i<set->n;i++)if(pf_is(&set->r[i],"procurement-draft:","service-site.room-purchase-list.v1"))lists++;
        if(lists>1){fputs("<nav class=\"ak-chips\" aria-label=\"Saved purchase lists\">",stdout);
            for(size_t i=0;i<set->n;i++){const PfRec *r=&set->r[i];if(!pf_is(r,"procurement-draft:","service-site.room-purchase-list.v1"))continue;char name[512];
                fputs("<a class=\"ak-chip\" href=\"",stdout);pf_href(binding,r->key);fputs("\"",stdout);if(r==f.list)fputs(" aria-current=\"true\"",stdout);fputs(">",stdout);
                if(pf_str(r,"title",name,sizeof(name))||pf_str(r,"name",name,sizeof(name)))html(name);else{fputs("List saved ",stdout);pf_str(r,"created_at",name,sizeof(name));pf_day(name);}fputs("</a>",stdout);}
            fputs("</nav>",stdout);}
        fputs("<div class=\"ak-table-wrap\"><table class=\"ak-table pf-items\"><thead><tr><th>Item</th><th>Option</th><th>Qty</th><th>Listed each</th><th>Listed total</th></tr></thead><tbody>",stdout);
        const bf_json_node_t *lines=pf_at(f.list->doc,f.list->root,"lines");
        for(const bf_json_node_t *l=lines&&lines->type==BF_JSON_ARRAY?bf_json_child_first(f.list->doc,lines):NULL;l;l=bf_json_child_next(f.list->doc,l)){
            char label[1024],option[1024];long long q=0,u=0,t=0;pf_nstr(f.list->doc,l,"label",label,sizeof(label));pf_nstr(f.list->doc,l,"option",option,sizeof(option));
            pf_int(f.list->doc,l,"quantity",&q);pf_int(f.list->doc,l,"unit_cents",&u);pf_int(f.list->doc,l,"subtotal_cents",&t);
            fputs("<tr><td>",stdout);html(label);fputs("</td><td>",stdout);html(option);printf("</td><td>%lld</td><td>",q);pf_money(u);fputs("</td><td>",stdout);pf_money(t);fputs("</td></tr>",stdout);
        }
        fputs("</tbody><tfoot><tr><td colspan=\"4\">Listed prices when the room was saved</td><td>",stdout);pf_money(list_total);fputs("</td></tr></tfoot></table></div>",stdout);
        if(!f.rfq){
            fputs("<form class=\"ak-actions\" method=\"post\" action=\"/apps/",stdout);html(binding);fputs("/_procurement_flow\">",stdout);
            psetup_hidden("action","request");psetup_hidden("list",lkey);
            fputs("<button type=\"submit\">Get quotes for these items</button><small>Makes a quote request you can share with suppliers yourself. Nothing is sent.</small></form>",stdout);
        }
    }
    fputs("</section>",stdout);

    /* 2 + 3. Compare quotes in one table, and pick one */
    long long best=-1;const PfRec *cheapest=NULL;
    for(size_t i=0;i<f.nquotes;i++){long long t;if(pf_quote_total(f.quotes[i],&t)&&(best<0||t<best)){best=t;cheapest=f.quotes[i];}}
    fputs("<section class=\"ak-pattern ak-card pf-stage\" id=\"quotes\">",stdout);
    pf_stage_head(2,"Compare quotes",f.approval?"Done":f.nquotes?"Now":f.rfq?"Now":"Next",f.rfq?"Type each supplier's prices into the empty column. The lowest complete total is marked.":"Opens once you ask for quotes.");
    if(f.rfq){
        int open=!f.approval;
        if(open){fputs("<form id=\"pf-add\" method=\"post\" action=\"/apps/",stdout);html(binding);fputs("/_procurement_flow\">",stdout);psetup_hidden("action","quote");psetup_hidden("list",lkey);psetup_hidden("rfq",f.rfq->key);fputs("</form>",stdout);
            fputs("<form id=\"pf-send\" method=\"post\" action=\"/apps/",stdout);html(binding);fputs("/_procurement_flow\">",stdout);psetup_hidden("action","send");psetup_hidden("list",lkey);psetup_hidden("rfq",f.rfq->key);fputs("</form>",stdout);}
        fputs("<div class=\"ak-table-wrap\"><table class=\"ak-table pf-compare\"><thead><tr><th>Item</th><th>Qty</th>",stdout);
        for(size_t i=0;i<f.nquotes;i++){fputs("<th class=\"pf-q",stdout);if(f.quotes[i]==cheapest)fputs(" is-best",stdout);fputs("\"><span>",stdout);pf_supplier(f.quotes[i]);fputs("</span>",stdout);
            if(f.quotes[i]==cheapest)fputs("<em>Lowest</em>",stdout);fputs("</th>",stdout);}
        if(open)fputs("<th class=\"pf-new\"><input form=\"pf-add\" name=\"supplier\" maxlength=\"200\" placeholder=\"Supplier name\" aria-label=\"Supplier name for a new quote\"></th>",stdout);
        fputs("</tr></thead><tbody>",stdout);
        const bf_json_node_t *rl=pf_at(f.rfq->doc,f.rfq->root,"lines");int index=0;
        for(const bf_json_node_t *l=rl&&rl->type==BF_JSON_ARRAY?bf_json_child_first(f.rfq->doc,rl):NULL;l;l=bf_json_child_next(f.rfq->doc,l),index++){
            char label[1024],line_id[65];long long q=0;pf_nstr(f.rfq->doc,l,"label",label,sizeof(label));pf_nstr(f.rfq->doc,l,"line_id",line_id,sizeof(line_id));pf_int(f.rfq->doc,l,"quantity_requested",&q);
            fputs("<tr><td>",stdout);html(label);printf("</td><td>%lld</td>",q);
            for(size_t i=0;i<f.nquotes;i++){long long u,e;fputs("<td class=\"pf-q\">",stdout);if(pf_quote_line(f.quotes[i],line_id,&u,&e)&&e>=0){pf_money(e);if(u>=0){fputs("<small>",stdout);pf_money(u);fputs(" each</small>",stdout);}}else fputs("<small>Not quoted</small>",stdout);fputs("</td>",stdout);}
            if(open)printf("<td class=\"pf-new\"><input form=\"pf-add\" name=\"price%d\" inputmode=\"decimal\" placeholder=\"Price each\" aria-label=\"Price each for ",index),html(label),fputs("\"></td>",stdout);
            fputs("</tr>",stdout);
        }
        const char *fees[][2]={{"freight_cents","Delivery"},{"tax_cents","Tax"},{"other_fees_cents","Other fees"}};const char *fields[]={"delivery","tax","fees"};
        for(int k=0;k<3;k++){
            fputs("<tr class=\"pf-fee\"><td colspan=\"2\">",stdout);html(fees[k][1]);fputs("</td>",stdout);
            for(size_t i=0;i<f.nquotes;i++){long long v;fputs("<td class=\"pf-q\">",stdout);if(pf_int(f.quotes[i]->doc,f.quotes[i]->root,fees[k][0],&v))pf_money(v);else fputs("<small>Unknown</small>",stdout);fputs("</td>",stdout);}
            if(open){fputs("<td class=\"pf-new\"><input form=\"pf-add\" name=\"",stdout);fputs(fields[k],stdout);fputs("\" inputmode=\"decimal\" placeholder=\"0.00\" aria-label=\"",stdout);html(fees[k][1]);fputs(" for the new quote\"></td>",stdout);}
            fputs("</tr>",stdout);
        }
        fputs("</tbody><tfoot><tr><td colspan=\"2\">Total</td>",stdout);
        for(size_t i=0;i<f.nquotes;i++){long long t;fputs("<td class=\"pf-q\">",stdout);if(pf_quote_total(f.quotes[i],&t))pf_money(t);else fputs("<small>Incomplete</small>",stdout);fputs("</td>",stdout);}
        if(open)fputs("<td class=\"pf-new\"><input form=\"pf-add\" name=\"source\" maxlength=\"400\" placeholder=\"Where it came from\" aria-label=\"Where the new quote came from\"></td>",stdout);
        fputs("</tr>",stdout);
        if(open&&f.nquotes){fputs("<tr class=\"pf-pick\"><td colspan=\"2\">Pick one</td>",stdout);
            for(size_t i=0;i<f.nquotes;i++){fputs("<td class=\"pf-q\"><label><input type=\"radio\" form=\"pf-send\" name=\"pick\" value=\"",stdout);html(f.quotes[i]->key);fputs("\"",stdout);if(f.quotes[i]==cheapest)fputs(" checked",stdout);fputs("> Choose</label></td>",stdout);}
            fputs("<td class=\"pf-new\"></td></tr>",stdout);}
        fputs("</tfoot></table></div>",stdout);
        if(open)fputs("<p class=\"ak-actions\"><button class=\"is-primary\" type=\"submit\" form=\"pf-add\">Add this quote</button><small>Prices in dollars. Use 0 when there is no delivery, tax or fee.</small></p>",stdout);
    }
    fputs("</section>",stdout);

    /* 4. Send for approval: the ledger */
    fputs("</div><aside class=\"ak-pattern ak-card pf-ledger\" id=\"send\"><p class=\"ak-eyebrow\">Send for approval</p>",stdout);
    if(f.approval){
        long long total=0;char at[32]="",decision[32]="";pf_int(f.approval->doc,f.approval->root,"known_total_cents",&total);pf_str(f.approval,"requested_at",at,sizeof(at));pf_str(f.approval,"authority.decision",decision,sizeof(decision));
        fputs("<p class=\"pf-total\">",stdout);pf_money(total);fputs("</p><dl class=\"ak-spec\">",stdout);
        const bf_json_node_t *lots=pf_at(f.approval->doc,f.approval->root,"order_lots");
        for(const bf_json_node_t *l=lots&&lots->type==BF_JSON_ARRAY?bf_json_child_first(f.approval->doc,lots):NULL;l;l=bf_json_child_next(f.approval->doc,l)){char name[512];long long t=0;
            if(!pf_nstr(f.approval->doc,l,"supplier.name",name,sizeof(name)))snprintf(name,sizeof(name),"Supplier");pf_int(f.approval->doc,l,"known_total_cents",&t);fputs("<dt>",stdout);html(name);fputs("</dt><dd>",stdout);pf_money(t);fputs("</dd>",stdout);}
        fputs("<dt>Sent</dt><dd>",stdout);pf_day(at);fputs("</dd><dt>Approval</dt><dd class=\"is-on\">",stdout);fputs(!strcmp(decision,"pending")?"Waiting":decision,stdout);
        fputs("</dd></dl><p class=\"pf-safe\">Nothing has been ordered or paid, and no supplier was contacted. Once approved, you place the order with the supplier yourself.</p>",stdout);
    }else if(f.nquotes){
        fputs("<p class=\"pf-total\">",stdout);if(best>=0)pf_money(best);else fputs("—",stdout);fputs("</p><dl class=\"ak-spec\">",stdout);
        if(cheapest){fputs("<dt>Lowest quote</dt><dd>",stdout);pf_supplier(cheapest);fputs("</dd>",stdout);}
        printf("<dt>Quotes</dt><dd>%zu</dd><dt>Listed</dt><dd>",f.nquotes);pf_money(list_total);fputs("</dd></dl>",stdout);
        fputs("<label class=\"pf-confirm\"><input type=\"checkbox\" form=\"pf-send\" name=\"confirm\" value=\"yes\" required> I checked the items, amounts and the quote I picked.</label>",stdout);
        fputs("<button class=\"is-primary\" type=\"submit\" form=\"pf-send\">Send for approval</button>",stdout);
        fputs("<p class=\"pf-safe\">Sending asks for approval in this workspace. It does not order, pay, or contact the supplier.</p>",stdout);
    }else{
        fputs("<p class=\"pf-total\">—</p><p class=\"pf-safe\">",stdout);
        fputs(f.rfq?"Add at least one quote with every price filled in. Then pick it here and send it for approval.":f.list?"Ask for quotes on your list first.":"Save a room's purchase list to begin.",stdout);
        fputs("</p>",stdout);
    }
    fputs("</aside></div>",stdout);
    ak_app_close();pf_free(set);free(set);
}

/* ---- the actions ---- */
static int pf_field(const char *body,const char *name,char *out,size_t cap){
    out[0]=0;size_t length=strlen(name);
    for(const char *p=body?body:"";*p;){const char *end=strchr(p,'&');if(!end)end=p+strlen(p);
        if((size_t)(end-p)>length&&!strncmp(p,name,length)&&p[length]=='=')return ac_decode(p+length+1,(size_t)(end-p-length-1),out,cap);
        p=*end?end+1:end;}
    return 0;
}
static int pf_cents(const char *text,long long *cents){
    const char *p=text;while(*p==' '||*p=='$')p++;if(!*p)return 0;long long whole=0;int digits=0;
    for(;*p>='0'&&*p<='9';p++){if(whole>9000000000LL)return 0;whole=whole*10+(*p-'0');digits++;}
    long long frac=0;if(*p=='.'){p++;int k=0;for(;*p>='0'&&*p<='9'&&k<2;p++,k++)frac=frac*10+(*p-'0');if(k==1)frac*=10;if(*p>='0'&&*p<='9')return 0;}
    while(*p==' ')p++;if(*p||!digits)return 0;*cents=whole*100+frac;return 1;
}
static void pf_redirect(const char *binding,const char *list_key,const char *note,const char *anchor){
    fputs("HTTP/1.1 303 See Other\r\nLocation: /apps/",stdout);html(binding);fputs("/procurement/?",stdout);
    if(list_key&&*list_key){fputs("list=",stdout);url_query_value(list_key);fputs("&",stdout);}fputs("note=",stdout);fputs(note,stdout);if(anchor){fputc('#',stdout);fputs(anchor,stdout);}
    fputs("\r\nCache-Control: private, no-store\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",stdout);
}
static int pf_act(const char *method,const char *query,const char *body,const char *raw,const char *host,const char *binding,const char *subject,const char *scope,const char *actor){
    if(strcmp(method,"POST")){aw_error(405,"Use the buying page to make this change.\n");return 1;}
    char origin[300],http[300],https[300],ctype[96];
    snprintf(http,sizeof(http),"http://%s",host?host:"");snprintf(https,sizeof(https),"https://%s",host?host:"");
    if(query&&*query){aw_error(400,"Send this from the buying page.\n");return 1;}
    if(!aw_header(raw,"Origin",origin,sizeof(origin))||(strcmp(origin,http)&&strcmp(origin,https))){aw_error(403,"Send this from the buying page on this site.\n");return 1;}
    if(!aw_header(raw,"Content-Type",ctype,sizeof(ctype))||strncmp(ctype,"application/x-www-form-urlencoded",33)||!body||strlen(body)>16384){aw_error(400,"Send one buying form.\n");return 1;}
    char action[16],list_key[512],rfq_key[512];pf_field(body,"action",action,sizeof(action));pf_field(body,"list",list_key,sizeof(list_key));pf_field(body,"rfq",rfq_key,sizeof(rfq_key));
    if(strncmp(list_key,"procurement-draft:",18)){aw_error(400,"Choose one saved purchase list.\n");return 1;}
    PfSet *set=calloc(1,sizeof(PfSet));if(!set||!pf_load(binding,subject,scope,actor,set)){free(set);aw_error(503,"Your saved purchase lists could not be read. Nothing was changed.\n");return 1;}
    const PfRec *list=pf_find(set,list_key);char now[21];pf_now(now);int rc=0;const char *done=NULL;
    if(!list||!pf_is(list,"procurement-draft:","service-site.room-purchase-list.v1")){pf_free(set);free(set);aw_error(404,"That purchase list is not in this workspace.\n");return 1;}
    if(!strcmp(action,"request")){
        /* The list must still describe the saved room it came from. */
        char source[512];long long want=0;pf_str(list,"source.key",source,sizeof(source));pf_int(list->doc,list->root,"source.revision",&want);
        unsigned char *answer=NULL;size_t length=0;AcPage *page=malloc(sizeof(AcPage));
        int current=page&&*source&&!pf_owner("get",binding,subject,scope,actor,source,"",NULL,0,&answer,&length)&&ac_page(answer,length,page)&&page->count==1&&page->rows[0].body&&page->rows[0].revision==want;
        free(page);free(answer);
        if(!current){pf_redirect(binding,list_key,"stale","need");pf_free(set);free(set);return 1;}
        char id[64];pf_id("rfq-",id,sizeof(id));bf_jsonw w;bf_jsonw_init(&w,0);bf_jsonw_obj(&w);
        bf_jsonw_key(&w,"purchase_list");bf_jsonw_node(&w,list->doc,list->root,0);bf_jsonw_key(&w,"purchase_list_revision");bf_jsonw_int(&w,list->rev);
        bf_jsonw_key(&w,"id");bf_jsonw_str(&w,id);bf_jsonw_key(&w,"requested_at");bf_jsonw_str(&w,now);bf_jsonw_key(&w,"supplier");bf_jsonw_null(&w);bf_jsonw_key(&w,"selected");bf_jsonw_null(&w);bf_jsonw_obj_end(&w);
        char *in=w.oom?NULL:strndup(w.buf,w.len);bf_jsonw_free(&w);char *rfq=in?pf_kernel("rfq",in,NULL):NULL;free(in);
        PfPut put;snprintf(put.key,sizeof(put.key),"procurement-rfq:%s",id);put.json=rfq;
        rc=rfq?pf_put(binding,subject,scope,actor,&put,1):6;free(rfq);done="requested";
    }else if(!strcmp(action,"quote")||!strcmp(action,"send")){
        const PfRec *rfq=pf_find(set,rfq_key);char list_id[160];pf_str(list,"id",list_id,sizeof(list_id));
        if(!rfq||!pf_is(rfq,"procurement-rfq:","bonfyre.procurement.supplier-request.v1")||!pf_eq(rfq,"source.purchase_list_id",list_id)){pf_free(set);free(set);aw_error(404,"That quote request is not in this workspace.\n");return 1;}
        if(!strcmp(action,"quote")){
            char supplier[256],source[512],text[64];pf_field(body,"supplier",supplier,sizeof(supplier));pf_field(body,"source",source,sizeof(source));
            if(!*supplier){pf_redirect(binding,list_key,"supplier","quotes");pf_free(set);free(set);return 1;}
            long long fees[3];const char *names[]={"delivery","tax","fees"};int ok=1;
            for(int k=0;k<3;k++)ok=ok&&pf_field(body,names[k],text,sizeof(text))&&pf_cents(text,&fees[k]);
            char qid[64];pf_id("quote-",qid,sizeof(qid));bf_jsonw w;bf_jsonw_init(&w,0);bf_jsonw_obj(&w);bf_jsonw_key(&w,"rfq");bf_jsonw_node(&w,rfq->doc,rfq->root,0);
            bf_jsonw_key(&w,"observation");bf_jsonw_obj(&w);bf_jsonw_key(&w,"id");bf_jsonw_str(&w,qid);bf_jsonw_key(&w,"supplier");bf_jsonw_obj(&w);bf_jsonw_key(&w,"name");bf_jsonw_str(&w,supplier);bf_jsonw_obj_end(&w);
            bf_jsonw_key(&w,"supplier_quote_id");bf_jsonw_null(&w);bf_jsonw_key(&w,"observed_at");bf_jsonw_str(&w,now);bf_jsonw_key(&w,"quoted_at");bf_jsonw_null(&w);bf_jsonw_key(&w,"valid_until");bf_jsonw_null(&w);
            bf_jsonw_key(&w,"currency");bf_jsonw_str(&w,"USD");bf_jsonw_key(&w,"source_ref");bf_jsonw_str(&w,*source?source:"Entered by hand");
            const char *fee_keys[]={"freight_cents","tax_cents","other_fees_cents"};for(int k=0;k<3;k++){bf_jsonw_key(&w,fee_keys[k]);if(ok)bf_jsonw_int(&w,fees[k]);else bf_jsonw_null(&w);}
            bf_jsonw_key(&w,"lines");bf_jsonw_arr(&w);const bf_json_node_t *rl=pf_at(rfq->doc,rfq->root,"lines");int index=0;
            for(const bf_json_node_t *l=rl&&rl->type==BF_JSON_ARRAY?bf_json_child_first(rfq->doc,rl):NULL;l;l=bf_json_child_next(rfq->doc,l),index++){
                char name[16],line_id[65],lid[64];long long q=0,unit=0;snprintf(name,sizeof(name),"price%d",index);
                if(!pf_field(body,name,text,sizeof(text))||!pf_cents(text,&unit))ok=0;pf_nstr(rfq->doc,l,"line_id",line_id,sizeof(line_id));pf_int(rfq->doc,l,"quantity_requested",&q);pf_id("quote-line-",lid,sizeof(lid));
                bf_jsonw_obj(&w);bf_jsonw_key(&w,"id");bf_jsonw_str(&w,lid);bf_jsonw_key(&w,"rfq_line_id");bf_jsonw_str(&w,line_id);bf_jsonw_key(&w,"quantity");bf_jsonw_int(&w,q);
                bf_jsonw_key(&w,"unit_cents");bf_jsonw_int(&w,unit);bf_jsonw_key(&w,"line_total_cents");bf_jsonw_null(&w);bf_jsonw_key(&w,"substitution");bf_jsonw_null(&w);
                bf_jsonw_key(&w,"availability");bf_jsonw_null(&w);bf_jsonw_key(&w,"lead_time");bf_jsonw_null(&w);bf_jsonw_obj_end(&w);
            }
            bf_jsonw_arr_end(&w);bf_jsonw_obj_end(&w);bf_jsonw_obj_end(&w);
            if(!ok){bf_jsonw_free(&w);pf_redirect(binding,list_key,"prices","quotes");pf_free(set);free(set);return 1;}
            char *in=w.oom?NULL:strndup(w.buf,w.len);bf_jsonw_free(&w);char *quote=in?pf_kernel("quote",in,NULL):NULL;free(in);
            PfPut put;snprintf(put.key,sizeof(put.key),"procurement-quote:%s",qid);put.json=quote;rc=quote?pf_put(binding,subject,scope,actor,&put,1):6;free(quote);done="quoted";
        }else{
            char pick[512],confirm[8];pf_field(body,"pick",pick,sizeof(pick));pf_field(body,"confirm",confirm,sizeof(confirm));
            const PfRec *quote=*pick?pf_find(set,pick):NULL;char rfq_id[160],rfq_digest[65];pf_str(rfq,"id",rfq_id,sizeof(rfq_id));pf_str(rfq,"digest",rfq_digest,sizeof(rfq_digest));
            if(!quote||!pf_is(quote,"procurement-quote:","bonfyre.procurement.supplier-quote-observation.v1")||!pf_eq(quote,"source.rfq_id",rfq_id)){pf_redirect(binding,list_key,"pick","quotes");pf_free(set);free(set);return 1;}
            if(strcmp(confirm,"yes")){pf_redirect(binding,list_key,"confirm","send");pf_free(set);free(set);return 1;}
            char qid[160],qdigest[65];pf_str(quote,"id",qid,sizeof(qid));pf_str(quote,"digest",qdigest,sizeof(qdigest));
            bf_jsonw w;bf_jsonw_init(&w,0);bf_jsonw_obj(&w);bf_jsonw_key(&w,"rfq");bf_jsonw_node(&w,rfq->doc,rfq->root,0);bf_jsonw_key(&w,"quotes");bf_jsonw_arr(&w);bf_jsonw_node(&w,quote->doc,quote->root,0);bf_jsonw_arr_end(&w);
            bf_jsonw_key(&w,"expected_rfq_digest");bf_jsonw_str(&w,rfq_digest);bf_jsonw_key(&w,"expected_quotes");bf_jsonw_arr(&w);bf_jsonw_obj(&w);bf_jsonw_key(&w,"id");bf_jsonw_str(&w,qid);bf_jsonw_key(&w,"digest");bf_jsonw_str(&w,qdigest);bf_jsonw_obj_end(&w);bf_jsonw_arr_end(&w);bf_jsonw_obj_end(&w);
            char *in=w.oom?NULL:strndup(w.buf,w.len);bf_jsonw_free(&w);char *comparison=in?pf_kernel("compare",in,now):NULL;free(in);
            char err[128];bf_json_doc_t *cdoc=comparison?bf_json_parse(comparison,strlen(comparison),err,sizeof(err)):NULL;char status[32]="";
            if(cdoc)pf_nstr(cdoc,bf_json_root(cdoc),"status",status,sizeof(status));
            if(!cdoc||strcmp(status,"current")){bf_json_free(cdoc);free(comparison);pf_redirect(binding,list_key,cdoc?"incomplete":"changed","quotes");pf_free(set);free(set);return 1;}
            char aid[64],pid[64],cid[64];pf_id("allocation-",aid,sizeof(aid));pf_id("approval-",pid,sizeof(pid));pf_id("",cid,sizeof(cid));
            bf_jsonw_init(&w,0);bf_jsonw_obj(&w);bf_jsonw_key(&w,"rfq");bf_jsonw_node(&w,rfq->doc,rfq->root,0);bf_jsonw_key(&w,"quotes");bf_jsonw_arr(&w);bf_jsonw_node(&w,quote->doc,quote->root,0);bf_jsonw_arr_end(&w);
            bf_jsonw_key(&w,"comparison");bf_jsonw_node(&w,cdoc,bf_json_root(cdoc),0);bf_jsonw_key(&w,"id");bf_jsonw_str(&w,aid);bf_jsonw_key(&w,"prepared_at");bf_jsonw_str(&w,now);bf_jsonw_key(&w,"policy");bf_jsonw_str(&w,"lowest_known_merchandise");bf_jsonw_obj_end(&w);
            in=w.oom?NULL:strndup(w.buf,w.len);bf_jsonw_free(&w);char *allocation=in?pf_kernel("allocate",in,NULL):NULL;free(in);bf_json_free(cdoc);
            char *approval=NULL;
            if(allocation){bf_json_doc_t *adoc=bf_json_parse(allocation,strlen(allocation),err,sizeof(err));
                if(adoc){bf_jsonw_init(&w,0);bf_jsonw_obj(&w);bf_jsonw_key(&w,"allocation");bf_jsonw_node(&w,adoc,bf_json_root(adoc),0);bf_jsonw_key(&w,"allocation_revision");bf_jsonw_int(&w,1);
                    bf_jsonw_key(&w,"id");bf_jsonw_str(&w,pid);bf_jsonw_key(&w,"requested_at");bf_jsonw_str(&w,now);bf_jsonw_key(&w,"requester");bf_jsonw_str(&w,actor);bf_jsonw_key(&w,"reason");bf_jsonw_null(&w);bf_jsonw_obj_end(&w);
                    in=w.oom?NULL:strndup(w.buf,w.len);bf_jsonw_free(&w);approval=in?pf_kernel("approval-request",in,NULL):NULL;free(in);bf_json_free(adoc);}}
            PfPut puts[3];snprintf(puts[0].key,sizeof(puts[0].key),"procurement-allocation:%s",aid);puts[0].json=allocation;
            snprintf(puts[1].key,sizeof(puts[1].key),"procurement-approval:%s",pid);puts[1].json=approval;snprintf(puts[2].key,sizeof(puts[2].key),"procurement-comparison:%s:%s",rfq_id,cid);puts[2].json=comparison;
            rc=approval?pf_put(binding,subject,scope,actor,puts,3):6;free(comparison);free(allocation);free(approval);done="sent";
        }
    }else{pf_free(set);free(set);aw_error(400,"Choose one buying step.\n");return 1;}
    pf_free(set);free(set);
    if(rc==4){pf_redirect(binding,list_key,"changed",NULL);return 1;}
    if(rc){aw_error(503,"This step could not be saved. Nothing was changed; try again.\n");return 1;}
    pf_redirect(binding,list_key,done,!strcmp(done,"sent")?"send":"quotes");return 1;
}
static int pf_query(const char *query,char *list_key,size_t cap,char *note,size_t note_cap){
    list_key[0]=note[0]=0;if(!query||!*query)return 1;if(strlen(query)>2048)return 0;
    pf_field(query,"list",list_key,cap);pf_field(query,"note",note,note_cap);return 1;
}
#endif
